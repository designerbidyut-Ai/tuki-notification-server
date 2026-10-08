const {createHash,randomUUID}=require('node:crypto');
const hash=value=>createHash('sha256').update(value).digest('hex');

function createNotificationDispatcher({db,messaging,now=Date.now}){
 const profiles=new Map(),pendingProfiles=new Map();
 async function senderProfile(id){
  const cached=profiles.get(id);if(cached&&now()-cached.at<60000)return cached.value;
  if(pendingProfiles.has(id))return pendingProfiles.get(id);
  const request=Promise.all([db.ref('profiles/'+id+'/name').get(),db.ref('profiles/'+id+'/avatarUrl').get()]).then(([name,photo])=>{
   const value={name:String(name.val()||'Your friend').slice(0,80),avatarUrl:typeof photo.val()==='string'&&photo.val().length<=2048&&/^https:\/\//i.test(photo.val())?photo.val():''};
   profiles.delete(id);profiles.set(id,{at:now(),value});while(profiles.size>256)profiles.delete(profiles.keys().next().value);return value;
  }).finally(()=>pendingProfiles.delete(id));pendingProfiles.set(id,request);return request;
 }

 async function allowed(event){
  const {recipient,sender,type,createdAt,startedAt}=event;
  if(!recipient||!sender||recipient===sender)return false;
  const paths=[`blocks/${recipient}/${sender}`,`blocks/${sender}/${recipient}`,`deletionRequests/${recipient}`,`deletionRequests/${sender}`];
  if((await Promise.all(paths.map(p=>db.ref(p).get()))).some(s=>s.exists()))return false;
  if(type==='message'){
   const [read,message]=await Promise.all([db.ref(`chatReads/${recipient}/${sender}`).get(),db.ref(`messages/${recipient}/${sender}/${event.id}`).get()]);
   const value=message.val();if(read.val()>=createdAt||!value||value.deleted||value.senderId!==sender)return false;
  }
  if(type==='friend'&&(await db.ref(`friendRequests/${recipient}/${sender}`).get()).val()!==createdAt)return false;
  if(type==='emergency'){
   const [signal,friend,reverse]=await Promise.all([db.ref(`emergencySignals/${sender}/startedAt`).get(),db.ref(`friends/${sender}/${recipient}`).get(),db.ref(`friends/${recipient}/${sender}`).get()]);
   if(signal.val()!==startedAt||!friend.exists()||!reverse.exists())return false;
  }
  return true;
 }
 async function deliver(event){
  if(!['message','friend','emergency'].includes(event.type))throw Error('Unsupported notification type');
  if(!await allowed(event))return {status:'suppressed',sent:0};
  const key=hash([event.type,event.recipient,event.sender,event.id].join(':'));
  const delivery=db.ref('pushDeliveries/'+key),lease=delivery.child('lease'),owner=randomUUID();
  const acquired=await lease.transaction(current=>current&&current.until>now()?undefined:{owner,until:now()+60000});
  if(!acquired.committed)return {status:'busy',sent:0};
  try{
   const [saved,devices,profile]=await Promise.all([delivery.child('tokens').get(),db.ref('pushTokens/'+event.recipient).get(),senderProfile(event.sender)]);
   const completed=saved.val()||{},tokens=devices.val()||{};
   const entries=Object.entries(tokens).filter(([,t])=>typeof t==='string'&&!completed[hash(t)]);
   if(!Object.keys(tokens).length)return {status:'no-tokens',sent:0};
   if(!entries.length)return {status:'done',sent:0};
   const pVal=profile||{};
   const rawName=typeof pVal==='string'?pVal:(pVal.name||'Your friend');
   const name=String(rawName||'Your friend').trim().slice(0,80)||'Your friend';
   const avatarUrl=typeof pVal.avatarUrl==='string'&&/^https?:\/\//i.test(pVal.avatarUrl)?pVal.avatarUrl:'';
   const title=event.type==='emergency'?'SOS · '+name:event.type==='friend'?'Friend request · '+name:name;
   const body=event.type==='emergency'?name+' needs help. Tap to view their location.':event.type==='friend'?name+' wants to connect with you on Tuki.':String(event.text||'New message').slice(0,160);
   const data={type:event.type,senderId:event.sender,eventId:key};
   if(event.type==='message'){data.messageId=event.id;data.createdAt=String(event.createdAt);}
   if(event.type==='emergency')data.startedAt=String(event.startedAt);
   if(avatarUrl)data.avatarUrl=avatarUrl;
   let sent=0,retry=false;
   for(let i=0;i<entries.length;i+=500){
    if(!await allowed(event))return {status:'suppressed',sent};
    const batch=entries.slice(i,i+500);
    const result=await messaging.sendEachForMulticast({tokens:batch.map(([,t])=>t),notification:{title,body,...(avatarUrl?{imageUrl:avatarUrl}:{})},data,android:{priority:'high',ttl:event.type==='emergency'?300000:3600000,notification:{channelId:event.type==='emergency'?'tuki-sos-v1':'tuki-messages-v1',tag:key,sound:'default',priority:'max',vibrateTimingsMillis:event.type==='emergency'?[0,500,200,500,200,500]:[0,160,100,160],...(avatarUrl?{imageUrl:avatarUrl}:{})}},apns:{payload:{aps:{sound:'default'}},...(avatarUrl?{fcmOptions:{imageUrl:avatarUrl}}:{})}});
    const acknowledged={};
    for(let j=0;j<batch.length;j++){
     const response=result.responses[j],[device,token]=batch[j],code=response.error?.code;
     const invalid=['messaging/registration-token-not-registered','messaging/invalid-registration-token'].includes(code);
     if(response.success){sent++;acknowledged[hash(token)]=now();}
     else if(invalid){acknowledged[hash(token)]=now();await db.ref(`pushTokens/${event.recipient}/${device}`).transaction(current=>current===null||current===token?null:undefined);}
     else retry=true;
    }
    if(Object.keys(acknowledged).length)await delivery.child('tokens').update(acknowledged);
   }
   if(retry)throw Error('Retryable push delivery failure');
   return {status:'done',sent};
  }finally{await lease.transaction(current=>current?.owner===owner?null:undefined);}
 }
 return {deliver};
}

function createNotificationWorker({db,messaging,now=Date.now,onError=()=>{},retryMs=5000,threadIndex}){
 const dispatcher=createNotificationDispatcher({db,messaging,now}),boot=now();
 const jobs=new Map(),timers=new Set(),offs=[],owners=new Map(),requestOffs=new Map();let closed=false;
 const stats={messagesProcessed:0,friendRequestsProcessed:0,sosProcessed:0,notificationsSent:0,errors:0,noTokenEvents:0};
 function enqueue(event){
  const id=[event.type,event.recipient,event.sender,event.id].join(':');if(closed||jobs.has(id))return;
  jobs.set(id,true);
  if(threadIndex&&event.type==='message')void threadIndex.incoming(event).catch(onError);
  const attempt=async count=>{
   if(closed){jobs.delete(id);return;}
   try{
    const result=await dispatcher.deliver(event);stats.notificationsSent+=result.sent;
    if(result.status==='busy'||result.status==='no-tokens'){
     if(result.status==='no-tokens')stats.noTokenEvents++;
     if(count<5){const timer=setTimeout(()=>{timers.delete(timer);void attempt(count+1);},retryMs*2**count);timers.add(timer);return;}
    }
   }catch(e){
    stats.errors++;onError(e);
    if(count<5){const timer=setTimeout(()=>{timers.delete(timer);void attempt(count+1);},retryMs*2**count);timers.add(timer);return;}
   }
   jobs.delete(id);
  };void attempt(0);
 }
 function listen(ref,event,callback){const fail=e=>{stats.errors++;onError(e);};ref.on(event,callback,fail);return()=>ref.off(event,callback);}
 function addOwner(snapshot){
  const owner=snapshot.key;if(owners.has(owner))return;
  const peers=new Map();const off=listen(db.ref('conversations/'+owner),'value',snap=>{
   threadIndex?.threads(owner,snap.val()||{});
   const ids=new Set(Object.keys(snap.val()||{}));
   for(const [peer,dispose]of peers)if(!ids.has(peer)){dispose();peers.delete(peer);}
   for(const peer of ids)if(!peers.has(peer)){
    const seen=new Set();peers.set(peer,listen(db.ref(`messages/${owner}/${peer}`).limitToLast(100),'value',messages=>{
     threadIndex?.messages(owner,peer,messages.val()||{});
     for(const [id,m]of Object.entries(messages.val()||{})){
      if(seen.has(id))continue;seen.add(id);
      // Process the recipient copy only. This covers new owners/peers and rapid messages.
      if(!m||m.senderId!==peer||m.deleted||!Number.isFinite(m.createdAt)||m.createdAt<boot-60000)continue;
      const event={type:'message',recipient:owner,sender:peer,id,text:m.text,createdAt:m.createdAt};stats.messagesProcessed++;enqueue(event);
     }
     if(seen.size>300){const keep=new Set(Object.keys(messages.val()||{}));for(const id of seen)if(!keep.has(id))seen.delete(id);}
    }));
   }
  });owners.set(owner,()=>{off();peers.forEach(dispose=>dispose());});
 }
 function addRequests(snapshot){
  const recipient=snapshot.key;if(requestOffs.has(recipient))return;
  let seen=new Map();requestOffs.set(recipient,listen(db.ref('friendRequests/'+recipient),'value',snap=>{
   const current=new Map(Object.entries(snap.val()||{}));
   for(const[sender,at]of current)if(seen.get(sender)!==at&&Number.isFinite(at)&&at>=boot-60000){stats.friendRequestsProcessed++;enqueue({type:'friend',recipient,sender,id:String(at),createdAt:at});}
   seen=current;
  }));
 }
 let signals=new Map();
 offs.push(listen(db.ref('conversations'),'child_added',addOwner));
 offs.push(listen(db.ref('conversations'),'child_removed',s=>{owners.get(s.key)?.();owners.delete(s.key);threadIndex?.removeOwner(s.key);}));
 offs.push(listen(db.ref('friendRequests'),'child_added',addRequests));
 offs.push(listen(db.ref('friendRequests'),'child_removed',s=>{requestOffs.get(s.key)?.();requestOffs.delete(s.key);}));
 offs.push(listen(db.ref('emergencySignals'),'value',snap=>{
  const current=new Map(Object.entries(snap.val()||{}).map(([sender,s])=>[sender,s?.startedAt]));
  for(const[sender,at]of current)if(signals.get(sender)!==at&&Number.isFinite(at)&&at>=boot-60000){
   void db.ref('friends/'+sender).get().then(friends=>{if(closed)return;for(const recipient of Object.keys(friends.val()||{})){stats.sosProcessed++;enqueue({type:'emergency',recipient,sender,id:String(at),startedAt:at});}}).catch(onError);
  }signals=current;
 }));
 return {stats,enqueue,stop(){closed=true;offs.forEach(off=>off());owners.forEach(off=>off());requestOffs.forEach(off=>off());timers.forEach(clearTimeout);timers.clear();},pendingCount:()=>jobs.size};
}
module.exports={createNotificationDispatcher,createNotificationWorker};
