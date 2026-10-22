/* QSync — state sync for the live quiz and the timer mirror
   (shared by the deck host, the participant page and timer.html).
   Transport: public MQTT brokers (retained, QoS1). Every message is a FULL STATE SNAPSHOT with a
   timestamp; receivers keep the newest. Clients publish/subscribe on every broker that is connected.
   - emqx / hivemq : WSS on 8084 / 8884
   - shiftr        : WSS on 443 (for hotel networks that block 8084/8884)
   ntfy.sh is NOT part of the normal path: its anonymous limits are per IP (≈250 messages/day,
   ~30 subscriptions, request bursts), so dozens of phones behind one hotel IP would exhaust it.
   - player: ntfy only as a last resort when NO broker has connected for >15 s (then 1 SSE + posts)
   - host  : at most one ntfy SSE subscription (to hear last-resort players); posts ctl to ntfy
             only while such players exist (decided by the caller)
   - timer : never uses ntfy (timer.html has a local mode)
   Privacy: topics are random and carry no names; retained MQTT data can be purged with empty payloads.
   ntfy messages cannot be deleted (cached ~12 h). */
(function(g){
  const BROKERS=[
    {id:'emqx',  url:'wss://broker.emqx.io:8084/mqtt'},
    {id:'hivemq',url:'wss://broker.hivemq.com:8884/mqtt'},
    {id:'shiftr',url:'wss://public.cloud.shiftr.io',opt:{username:'public',password:'public'}}
  ];
  const NTFY='https://ntfy.sh/';
  const NS='nvq';                 // neutral MQTT namespace (host, player and timer page must match)
  const FALLBACK_AFTER=15000;     // player: ms without any MQTT connection before ntfy is used
  const rid=()=>Math.random().toString(36).slice(2,10);
  function fetchTO(url,opt,ms){const ac=new AbortController();const t=setTimeout(()=>ac.abort(),ms||6000);
    return fetch(url,Object.assign({},opt,{signal:ac.signal})).finally(()=>clearTimeout(t))}

  function create(o){
    // o: {topic, role:'host'|'player'|'timer', only:[broker ids], onCtl(m,src,retained), onPlayer(m,src), onTimer(m,src,retained), onStatus(st), onReconnect(id), onFallback(on)}
    const base=NS+'/'+o.topic;
    const SUB={host:base+'/p/+',player:base+'/ctl',timer:base+'/tm'}[o.role];
    const NCH={host:'-p',player:'-ctl'}[o.role];
    const list=BROKERS.filter(b=>!o.only||!o.only.length||o.only.includes(b.id));
    const st={}; list.forEach(b=>st[b.id]='connecting'); if(o.role!=='timer')st.ntfy='off';
    const clients={}; let es=null, stopped=false, esTimer=null, fb=false, lastMq=Date.now();
    const emit=()=>{try{o.onStatus&&o.onStatus(Object.assign({},st))}catch(e){}};
    const anyUp=()=>Object.values(clients).some(c=>c.connected);
    function handle(raw,src,retained){
      if(!raw)return; let m; try{m=typeof raw==='string'?JSON.parse(raw):raw}catch(e){return}
      if(!m||typeof m!=='object')return;
      try{
        if(m.k==='ctl'&&o.onCtl)o.onCtl(m,src,retained);
        else if(m.k==='p'&&o.onPlayer)o.onPlayer(m,src);
        else if(m.k==='tm'&&o.onTimer)o.onTimer(m,src,retained);
      }catch(e){console.error(e)}
    }
    function mqttStart(){
      if(!g.mqtt){list.forEach(b=>st[b.id]='unavailable');emit();return}
      list.forEach(b=>{
        let c;
        try{c=g.mqtt.connect(b.url,Object.assign({clientId:o.role+'-'+rid(),clean:true,keepalive:30,reconnectPeriod:3000,connectTimeout:10000,resubscribe:true},b.opt||{}));}
        catch(e){st[b.id]='error';emit();return}
        clients[b.id]=c;
        c.on('connect',()=>{st[b.id]='live';lastMq=Date.now();emit();
          c.subscribe(SUB,{qos:1});
          if(fb)setFallback(false);
          if(o.onReconnect)try{o.onReconnect(b.id)}catch(e){}
        });
        c.on('reconnect',()=>{st[b.id]='connecting';emit()});
        c.on('close',()=>{if(st[b.id]==='live'){st[b.id]='connecting';emit()}});
        c.on('offline',()=>{st[b.id]='error';emit()});
        c.on('error',()=>{st[b.id]='error';emit()});
        c.on('message',(t,p,pk)=>{const s=p&&p.toString();if(s)handle(s,b.id,!!(pk&&pk.retain))});
      });
    }
    function ntfyStart(){
      if(stopped||!NCH)return;
      try{es&&es.close()}catch(e){}
      st.ntfy='connecting';emit();
      es=new EventSource(NTFY+o.topic+NCH+'/sse?since=all');
      es.onopen=()=>{st.ntfy='live';emit()};
      es.onmessage=ev=>{let d;try{d=JSON.parse(ev.data)}catch(e){return}if(d.event==='message')handle(d.message,'ntfy',true)};
      es.onerror=()=>{st.ntfy=es.readyState===2?'error':'connecting';emit();
        if(es.readyState===2){clearTimeout(esTimer);esTimer=setTimeout(()=>{if(o.role==='host'||fb)ntfyStart()},5000)}};
    }
    function ntfyStop(){clearTimeout(esTimer);try{es&&es.close()}catch(e){}es=null;st.ntfy='off';emit()}
    function setFallback(on){
      if(o.role!=='player'||fb===on)return;fb=on;
      if(on)ntfyStart();else ntfyStop();
      try{o.onFallback&&o.onFallback(on)}catch(e){}
    }
    function mqttPub(topic,payload,retain){
      const ids=Object.keys(clients).filter(id=>clients[id].connected);
      if(!ids.length)return Promise.resolve(false);
      const msg=typeof payload==='string'?payload:JSON.stringify(payload);
      return new Promise(res=>{let left=ids.length,ok=false;const fin=()=>{if(--left===0)res(ok)};
        const to=setTimeout(()=>res(ok),4000);
        ids.forEach(id=>{try{clients[id].publish(topic,msg,{qos:1,retain:!!retain},err=>{if(!err){ok=true;clearTimeout(to);res(true)}fin()})}catch(e){fin()}});
      });
    }
    async function ntfyPub(ch,obj){
      for(let i=0;i<2;i++){
        try{const r=await fetchTO(NTFY+ch,{method:'POST',body:JSON.stringify(obj)},6000);if(r.ok)return true;if(r.status===429)return false}catch(e){}
        await new Promise(r=>setTimeout(r,800*(i+1)));
      }
      return false;
    }
    const api={
      start(){mqttStart();if(o.role==='host')ntfyStart();emit();
        if(o.role==='player')setInterval(()=>{if(anyUp()){lastMq=Date.now();if(fb)setFallback(false)}else if(!fb&&Date.now()-lastMq>FALLBACK_AFTER)setFallback(true)},2000);
        document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible'){
          if(es&&es.readyState===2)ntfyStart();
          Object.values(clients).forEach(c=>{if(!c.connected)try{c.reconnect()}catch(e){}});
        }});
      },
      status(){return Object.assign({},st)},
      mqttUp:anyUp,
      fallback(){return fb},
      // host: full ctl state. MQTT retained always; ntfy only if useNtfy (last-resort players exist) or no broker is up
      async publishCtl(state,useNtfy){
        state.k='ctl';
        const a=await mqttPub(base+'/ctl',state,true);
        let b=false;if(useNtfy||!a)b=await ntfyPub(o.topic+'-ctl',state);
        return a||b;
      },
      // host: one-off (non-retained) ctl, e.g. the "purged" signal before deleting retained data
      ctlOnce(state){state.k='ctl';return mqttPub(base+'/ctl',state,false)},
      // player: remove own retained snapshot
      clearOwn(pid){return /^[\w-]{1,40}$/.test(pid)?mqttPub(base+'/p/'+pid,'',true):Promise.resolve(false)},
      // host: timer mirror state (MQTT only)
      publishTimer(state){state.k='tm';return mqttPub(base+'/tm',state,true)},
      // player: MQTT; ntfy only while in last-resort fallback (mode 'never' = MQTT only)
      async publishPlayer(snap,mode){
        snap.k='p';
        const a=await mqttPub(base+'/p/'+snap.pid,snap,true);
        let b=false;if(!a&&fb&&mode!=='never')b=await ntfyPub(o.topic+'-p',snap);
        return a||b;
      },
      // host-side helper for rehearsal bots (MQTT only)
      publishAsPlayer(snap){snap.k='p';return mqttPub(base+'/p/'+snap.pid,snap,true)},
      // host: remove retained MQTT data of this topic (ctl, tm and every given player id) on all connected brokers
      async purge(pids){
        const topics=[base+'/ctl',base+'/tm',...[...new Set(pids||[])].filter(p=>/^[\w-]{1,40}$/.test(p)).map(p=>base+'/p/'+p)];
        const one=(c,t)=>new Promise(r=>{const to=setTimeout(()=>r(false),5000);try{c.publish(t,'',{qos:1,retain:true},e=>{clearTimeout(to);r(!e)})}catch(e){clearTimeout(to);r(false)}});
        const res={};
        for(let pass=0;pass<2;pass++){            // two passes per broker (covers a broker reconnecting mid-purge)
          for(const [id,c] of Object.entries(clients)){if(!c.connected){res[id]=res[id]||0;continue}
            let n=0;for(let i=0;i<topics.length;i+=20){const r=await Promise.all(topics.slice(i,i+20).map(t=>one(c,t)));n+=r.filter(Boolean).length}
            res[id]=Math.max(res[id]||0,n)}
          if(pass===0)await new Promise(r=>setTimeout(r,1500));
        }
        /* sweep: find any retained leftovers under this topic on each broker and clear them too */
        let swept=0;
        for(const c of Object.values(clients)){if(!c.connected)continue;
          const found=await new Promise(done=>{const set=new Set();const h=(t,p,pk)=>{if(t.startsWith(base+'/')&&pk&&pk.retain&&p&&p.length)set.add(t)};
            c.on('message',h);c.subscribe(base+'/#',{qos:1});setTimeout(()=>{c.removeListener('message',h);try{c.unsubscribe(base+'/#')}catch(e){}done([...set])},3500)});
          for(const t of found){if(await one(c,t))swept++}}
        return {topics:topics.length,ok:Math.max(0,...Object.values(res)),perBroker:res,swept};
      },
      stop(){stopped=true;ntfyStop();Object.values(clients).forEach(c=>{try{c.end(true)}catch(e){}})}
    };
    return api;
  }
  g.QSync={create,BROKERS,NS};
})(window);
