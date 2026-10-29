const {createHash,randomUUID}=require('node:crypto');
const hash=value=>createHash('sha256').update(value).digest('hex');
const {normalizeEvent,eventKey,expiresAt}=require('./notification-policy.cjs');
const {createNotificationQueue}=require('./notification-queue.cjs');
const {cityEventAllowed,reserveCityBudget}=require('./city-join.cjs');

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
  if(type==='city_join')return cityEventAllowed(db,event,now());
  if(type==='message'){
   const [read,message]=await Promise.all([db.ref(`chatReads/${recipient}/${sender}`).get(),db.ref(`messages/${recipient}/${sender}/${event.id}`).get()]);
   const value=message.val();if(read.val()>=createdAt||!value||value.deleted||value.senderId!==sender||value.createdAt!==createdAt)return false;
   event.text=String(value.text||'New message').slice(0,160);
  }
  if(type==='circle'){
   const [group,read,message,hidden]=await Promise.all([db.ref(`circles/${event.circleId}`).get(),db.ref(`circleReads/${event.circleId}/${recipient}`).get(),db.ref(`circleMessages/${event.circleId}/${event.id}`).get(),db.ref(`circleHidden/${recipient}/${event.circleId}/${event.id}`).get()]);
   const g=group.val(),m=message.val();
   if(g?.members?.[recipient]!==true||g.members?.[sender]!==true||read.val()>=createdAt||hidden.val()===true||!m||m.deleted||m.senderId!==sender||m.createdAt!==createdAt)return false;
   event.circleName=String(g.name||'Circle').slice(0,60);event.text=String(m.text||'New message').slice(0,160);
  }
  if(type==='friend'&&(await db.ref(`friendRequests/${recipient}/${sender}`).get()).val()!==createdAt)return false;
  if(type==='emergency'){
   const [signal,friend,reverse]=await Promise.all([db.ref(`emergencySignals/${sender}/startedAt`).get(),db.ref(`friends/${sender}/${recipient}`).get(),db.ref(`friends/${recipient}/${sender}`).get()]);
   if(signal.val()!==startedAt||!friend.exists()||!reverse.exists())return false;
  }
  return true;
 }
 async function deliver(input){
  if(!['message','circle','friend','emergency','city_join'].includes(input.type))throw Error('Unsupported notification type');
  const event=normalizeEvent(input,now());if(!event)return {status:'suppressed',sent:0};
  if(!await allowed(event))return {status:'suppressed',sent:0};
  const key=eventKey(event);
  const delivery=db.ref('pushDeliveries/'+key),lease=delivery.child('lease'),owner=randomUUID();
  const acquired=await lease.transaction(current=>current&&current.until>now()?undefined:{owner,until:now()+60000});
  if(!acquired.committed)return {status:'busy',sent:0};
  try{
   const [saved,devices,profile]=await Promise.all([delivery.child('tokens').get(),db.ref('pushTokens/'+event.recipient).get(),senderProfile(event.sender)]);
   const completed=saved.val()||{},tokens=devices.val()||{};
   const cityDevices=event.type==='city_join'?(await db.ref('cityJoinDevices/'+event.recipient).get()).val()||{}:null;
   const entries=Object.entries(tokens).filter(([device,t])=>typeof t==='string'&&!completed[hash(t)]&&(!cityDevices||cityDevices[device]===hash(t)));
   if(!Object.keys(tokens).length)return {status:'no-tokens',sent:0};
   if(!entries.length)return {status:'done',sent:0};
   if(event.type==='city_join'&&!await reserveCityBudget(db,event.recipient,key,now()))return {status:'suppressed',sent:0};
   const pVal=profile||{};
   const rawName=typeof pVal==='string'?pVal:(pVal.name||'Your friend');
   const name=String(rawName||'Your friend').trim().slice(0,80)||'Your friend';
   const avatarUrl=typeof pVal.avatarUrl==='string'&&/^https?:\/\//i.test(pVal.avatarUrl)?pVal.avatarUrl:'';
   const title=event.type==='city_join'?name+' joined Tuki':event.type==='circle'?event.circleName+' · '+name:event.type==='emergency'?'SOS · '+name:event.type==='friend'?'Friend request · '+name:name;
   const body=event.type==='city_join'?'New in '+event.city+' · Tap to view profile.':event.type==='emergency'?name+' needs help. Tap to view their location.':event.type==='friend'?name+' wants to connect with you on Tuki.':String(event.text||'New message').slice(0,160);
   const data={type:event.type,senderId:event.sender,ownerId:event.recipient,eventId:key};
   if(event.type==='message'||event.type==='circle'){data.messageId=event.id;data.createdAt=String(event.createdAt);}
   if(event.type==='circle'){data.circleId=event.circleId;data.threadId=event.circleId;}
   if(event.type==='emergency')data.startedAt=String(event.startedAt);
   if(avatarUrl)data.avatarUrl=avatarUrl;
   let sent=0,retry=false;
   for(let i=0;i<entries.length;i+=500){
    if(!normalizeEvent(event,now())||!await allowed(event))return {status:'suppressed',sent};
    const batch=entries.slice(i,i+500);
    const result=await messaging.sendEachForMulticast({tokens:batch.map(([,t])=>t),notification:{title,body,...(avatarUrl?{imageUrl:avatarUrl}:{})},data,android:{priority:event.type==='city_join'?'normal':'high',ttl:Math.max(1,expiresAt(event)-now()),notification:{channelId:event.type==='city_join'?'tuki-city-joins-v1':event.type==='emergency'?'tuki-sos-v1':'tuki-messages-v1',tag:key,sound:'default',priority:event.type==='city_join'?'default':'max',vibrateTimingsMillis:event.type==='emergency'?[0,500,200,500,200,500]:[0,160,100,160],...(avatarUrl?{imageUrl:avatarUrl}:{})}},apns:{headers:{'apns-expiration':String(Math.floor(expiresAt(event)/1000))},payload:{aps:{sound:'default'}},...(avatarUrl?{fcmOptions:{imageUrl:avatarUrl}}:{})}});
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
 const dispatcher=createNotificationDispatcher({db,messaging,now});
 const offs=[],owners=new Map(),requestOffs=new Map(),circleOffs=new Map(),sourceRetries=new Map();let closed=false;
 const stats={messagesProcessed:0,circleMessagesProcessed:0,friendRequestsProcessed:0,sosProcessed:0,notificationsSent:0,errors:0,noTokenEvents:0};
 const fail=e=>{stats.errors++;onError(e);};
 const queue=createNotificationQueue({db,now,retryMs,onError:fail,deliver:async event=>{
  const result=await dispatcher.deliver(event);stats.notificationsSent+=result.sent;
  if(result.status==='no-tokens')stats.noTokenEvents++;return result;
 }});
 async function enqueue(event){
  if(closed)return false;
  const accepted=await queue.enqueue(event);
  if(accepted&&threadIndex&&event.type==='message')void threadIndex.incoming(event).catch(fail);
  return accepted;
 }
 // A database write failure must not mark the canonical event permanently seen.
 function submit(event){
  if(closed||!normalizeEvent(event,now()))return;
  const key=eventKey(event);if(sourceRetries.has(key))return;
  sourceRetries.set(key,null);
  void enqueue(event).then(()=>sourceRetries.delete(key)).catch(e=>{
   fail(e);if(closed){sourceRetries.delete(key);return;}
   const timer=setTimeout(()=>{sourceRetries.delete(key);submit(event);},retryMs);timer.unref?.();sourceRetries.set(key,timer);
  });
 }
 function listen(ref,event,callback){const fail=e=>{stats.errors++;onError(e);};ref.on(event,callback,fail);return()=>ref.off(event,callback);}
 function addOwner(snapshot){
  const owner=snapshot.key;if(owners.has(owner))return;
  const peers=new Map();const off=listen(db.ref('conversations/'+owner),'value',snap=>{
   threadIndex?.threads(owner,snap.val()||{});
   const ids=new Set(Object.keys(snap.val()||{}));
   for(const [peer,dispose]of peers)if(!ids.has(peer)){dispose();peers.delete(peer);}
   for(const peer of ids)if(!peers.has(peer)){
    const seen=new Set();peers.set(peer,listen(db.ref(`messages/${owner}/${peer}`).orderByChild('createdAt').limitToLast(100),'value',messages=>{
     threadIndex?.messages(owner,peer,messages.val()||{});
     for(const [id,m]of Object.entries(messages.val()||{})){
      if(seen.has(id))continue;seen.add(id);
      // Process the recipient copy only. This covers new owners/peers and rapid messages.
      if(!m||m.senderId!==peer||m.deleted)continue;
      const event={type:'message',recipient:owner,sender:peer,id,createdAt:m.createdAt};if(!normalizeEvent(event,now()))continue;stats.messagesProcessed++;submit(event);
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
   for(const[sender,at]of current)if(seen.get(sender)!==at&&Number.isFinite(at)){const event={type:'friend',recipient,sender,id:String(at),createdAt:at};if(normalizeEvent(event,now())){stats.friendRequestsProcessed++;submit(event);}}
   seen=current;
  }));
 }
 function addCircle(snapshot){
  const circleId=snapshot.key,g=snapshot.val();if(closed||!g?.members)return;
  const existing=circleOffs.get(circleId);if(existing){existing.members=g.members;return;}
  const state={members:g.members,seen:new Set(),off:null};circleOffs.set(circleId,state);
  state.off=listen(db.ref('circleMessages/'+circleId).orderByChild('createdAt').limitToLast(100),'value',snapshot=>{
   const messages=snapshot.val()||{};
   for(const[id,m]of Object.entries(messages)){
    if(state.seen.has(id))continue;state.seen.add(id);if(!m||m.deleted||state.members[m.senderId]!==true)continue;
    for(const recipient of Object.keys(state.members))if(state.members[recipient]===true&&recipient!==m.senderId){
     const event={type:'circle',circleId,recipient,sender:m.senderId,id,createdAt:m.createdAt};
     if(normalizeEvent(event,now())){stats.circleMessagesProcessed++;submit(event);}
    }
   }
   if(state.seen.size>300)for(const id of state.seen)if(!messages[id])state.seen.delete(id);
  });
 }
 let signals=new Map();
 offs.push(listen(db.ref('conversations'),'child_added',addOwner));
 offs.push(listen(db.ref('conversations'),'child_removed',s=>{owners.get(s.key)?.();owners.delete(s.key);threadIndex?.removeOwner(s.key);}));
 offs.push(listen(db.ref('friendRequests'),'child_added',addRequests));
 offs.push(listen(db.ref('friendRequests'),'child_removed',s=>{requestOffs.get(s.key)?.();requestOffs.delete(s.key);}));
 offs.push(listen(db.ref('circles'),'child_added',addCircle));
 offs.push(listen(db.ref('circles'),'child_changed',addCircle));
 offs.push(listen(db.ref('circles'),'child_removed',s=>{circleOffs.get(s.key)?.off?.();circleOffs.delete(s.key);}));
 offs.push(listen(db.ref('emergencySignals'),'value',snap=>{
  const current=new Map(Object.entries(snap.val()||{}).map(([sender,s])=>[sender,s?.startedAt]));
  for(const[sender,at]of current)if(signals.get(sender)!==at&&Number.isFinite(at)&&at>now()-300000&&at<=now()+60000){
   void db.ref('friends/'+sender).get().then(friends=>{if(closed)return;for(const recipient of Object.keys(friends.val()||{})){stats.sosProcessed++;submit({type:'emergency',recipient,sender,id:String(at),startedAt:at});}}).catch(fail);
  }signals=current;
 }));
 return {stats,enqueue,stop(){closed=true;offs.forEach(off=>off());owners.forEach(off=>off());requestOffs.forEach(off=>off());circleOffs.forEach(state=>state.off?.());sourceRetries.forEach(timer=>timer&&clearTimeout(timer));sourceRetries.clear();queue.stop();},pendingCount:()=>queue.pendingCount()+sourceRetries.size};
}
module.exports={createNotificationDispatcher,createNotificationWorker};
