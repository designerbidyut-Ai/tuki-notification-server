const {test}=require('node:test'),assert=require('node:assert/strict');
const {createNotificationDispatcher,createNotificationWorker}=require('./notification-core.cjs');
const clone=v=>v==null?v:JSON.parse(JSON.stringify(v));
function fakeDatabase(initial={}){
 const data=new Map(Object.entries(initial)),listeners=new Map(),reads=[];
 const store=(path,val)=>{for(const key of data.keys())if(key.startsWith(path+'/'))data.delete(key);data.set(path,val);};
 function value(path){
  let result=null;
  for(const [key,val]of [...data].sort((a,b)=>a[0].length-b[0].length)){
   if(path===key)result=clone(val);
   else if(path.startsWith(key+'/')){let next=val;for(const bit of path.slice(key.length+1).split('/'))next=next?.[bit];result=clone(next??null);}
   else if(key.startsWith(path+'/')){result=result&&typeof result==='object'?result:{};let node=result;const bits=key.slice(path.length+1).split('/');for(const bit of bits.slice(0,-1))node=node[bit]||=(Object.create(null));if(val===null)delete node[bits.at(-1)];else node[bits.at(-1)]=clone(val);}
  }return result;
 }
 const snap=(path,val,key=path.split('/').at(-1))=>({key,val:()=>clone(val),exists:()=>val!=null});
 function ref(path){return {
  child:s=>ref(path+'/'+s),get:async()=>{reads.push(path);return snap(path,value(path));},orderByChild:()=>ref(path),endAt:()=>ref(path),limitToFirst:()=>ref(path),limitToLast:()=>ref(path),set:async val=>store(path,val),remove:async()=>store(path,null),
  update:async fields=>{for(const[k,v]of Object.entries(fields))store(path+'/'+k,v);},
  transaction:async fn=>{const next=fn(value(path));if(next===undefined)return {committed:false};store(path,next);return {committed:true,snapshot:snap(path,next)};},
  on(event,callback){const key=path+':'+event;const list=listeners.get(key)||[];list.push(callback);listeners.set(key,list);queueMicrotask(()=>{if(!list.includes(callback))return;const val=value(path);if(event==='value')callback(snap(path,val));else if(event==='child_added')for(const[k,v]of Object.entries(val||{}))callback(snap(path,v,k));});},
  off(event,callback){const list=listeners.get(path+':'+event)||[];const i=list.indexOf(callback);if(i>=0)list.splice(i,1);},
 };}
 return {ref,value,reads,set:store,emit(path,event,val,key){if(event==='value')store(path,val);for(const fn of listeners.get(path+':'+event)||[])fn(snap(path,val,key));}};
}
const settle=async()=>{for(let i=0;i<20;i++)await new Promise(setImmediate);};
test('follower changes use the durable per-owner index instead of repeated global downloads',async()=>{
 const db=fakeDatabase({following:{alice:{bob:true}},followers:{bob:{alice:true}}});
 let stop=require('./followers-sync.cjs').startFollowersSync({db,onError:e=>{throw e;}});await settle();
 assert.equal(db.reads.filter(p=>p==='followers').length,1);
 for(let i=0;i<50;i++){db.emit('following','child_changed',{bob:true},'alice');await settle();}
 assert.equal(db.reads.filter(p=>p==='followers').length,1);assert.equal(db.value('followerSyncState/alice').version,1);stop();
 stop=require('./followers-sync.cjs').startFollowersSync({db,onError:e=>{throw e;}});await settle();assert.equal(db.reads.filter(p=>p==='followers').length,1);stop();
});
test('notification retries reuse small sender fields and never read the legacy cover',async()=>{
 const s=setup();s.db.set('profiles/alice/coverUrl','data:image/jpeg;base64,'+'A'.repeat(200000));
 const worker=createNotificationDispatcher({db:s.db,messaging:s.messaging,now:()=>s.time});
 await worker.deliver({type:'message',recipient:'bob',sender:'alice',id:'m1',text:'Hello',createdAt:s.time});
 s.db.set('messages/bob/alice/m2',{senderId:'alice',text:'Second',createdAt:s.time});
 await worker.deliver({type:'message',recipient:'bob',sender:'alice',id:'m2',text:'Second',createdAt:s.time});
 assert.equal(s.sent.length,2);assert.equal(s.db.reads.filter(p=>p==='profiles/alice/name').length,1);assert.equal(s.db.reads.filter(p=>p==='profiles/alice/avatarUrl').length,1);assert.ok(!s.db.reads.includes('profiles/alice'));
});
test('authenticated wake recovers a cold-start message, deduplicates, and rejects forged or old references',async()=>{
 const s=setup(),worker=createNotificationWorker({db:s.db,messaging:s.messaging,now:()=>s.time+90000});
 const handler=require('./message-wake.cjs').createMessageWake({auth:{verifyIdToken:async token=>{if(token!=='valid')throw Error('invalid');return{uid:'alice'};}},db:s.db,worker,now:()=>s.time+90000});
 const call=async(body,token='valid')=>{let code;const res={status(value){code=value;return this;},json(){return code;}};return handler({body,get:()=>token?'Bearer '+token:''},res);};
 assert.equal(await call({recipient:'bob',messageId:'m1'},''),401);
 assert.equal(await call({recipient:'bob',messageId:'m1'},'invalid'),401);
 assert.equal(await call({recipient:'bob/other',messageId:'m1'}),400);
 assert.equal(await call({recipient:'bob',messageId:'missing'}),404);
 assert.equal(await call({recipient:'alice',messageId:'m1'}),400);
 assert.equal(await call({recipient:'bob',messageId:'m1'}),202);await settle();
 assert.equal(s.sent.length,1);
 assert.equal(await call({recipient:'bob',messageId:'m1'}),202);await settle();assert.equal(s.sent.length,1);
 s.db.set('messages/bob/alice/m1',{senderId:'mallory',text:'Forged',createdAt:s.time});
 assert.equal(await call({recipient:'bob',messageId:'m1'}),404);
 s.db.set('messages/bob/alice/m1',{senderId:'alice',text:'Old',createdAt:1});
 assert.equal(await call({recipient:'bob',messageId:'m1'}),404);worker.stop();
});
test('existing follower mirror survives notification repairs and removes unfollows',async()=>{
 const db=fakeDatabase({following:{alice:{bob:true}},followers:{}});let synced=0;
 const stop=require('./followers-sync.cjs').startFollowersSync({db,onSynced:()=>synced++,onError:e=>{throw e;}});await settle();
 assert.equal(db.value('followers/bob/alice'),true);assert.equal(synced,1);
 db.emit('following','child_changed',{bob:true},'alice');await settle();assert.equal(synced,1);
 db.set('following/alice',null);db.emit('following','child_removed',null,'alice');await settle();assert.equal(db.value('followers/bob/alice'),null);
 stop();db.set('following/alice',{bob:true});db.emit('following','child_added',{bob:true},'alice');await settle();assert.equal(db.value('followers/bob/alice'),null);
});
function setup(extra={}){
 const time=1700000000000,event={type:'message',recipient:'bob',sender:'alice',id:'m1',text:'Hello',createdAt:time};
 const db=fakeDatabase({'profiles/alice/name':'Alice','pushTokens/bob':{phone:'token-bob'},'messages/bob/alice/m1':{senderId:'alice',text:'Hello',createdAt:time},...extra});
 const sent=[];let respond=payload=>({responses:payload.tokens.map(()=>({success:true}))});
 const messaging={sendEachForMulticast:async payload=>{sent.push(payload);return respond(payload);}};
 return {db,sent,messaging,time,event,setRespond:fn=>respond=fn,dispatcher:createNotificationDispatcher({db,messaging,now:()=>time})};
}
test('message carries name, sound, vibration and stable message identity; restart does not resend',async()=>{
 const s=setup();await s.dispatcher.deliver(s.event);await createNotificationDispatcher({db:s.db,messaging:s.messaging,now:()=>s.time}).deliver(s.event);
 assert.equal(s.sent.length,1);assert.equal(s.sent[0].notification.title,'Alice');assert.equal(s.sent[0].data.messageId,'m1');assert.equal(s.sent[0].android.notification.sound,'default');assert.deepEqual(s.sent[0].android.notification.vibrateTimingsMillis,[0,160,100,160]);
});
test('sender profile photo is included in notification image and data payloads when present',async()=>{
 const s=setup({'profiles/alice':{name:'Alice',avatarUrl:'https://example.com/alice.jpg'}});
 await s.dispatcher.deliver(s.event);
 assert.equal(s.sent.length,1);
 assert.equal(s.sent[0].notification.imageUrl,'https://example.com/alice.jpg');
 assert.equal(s.sent[0].android.notification.imageUrl,'https://example.com/alice.jpg');
 assert.equal(s.sent[0].data.avatarUrl,'https://example.com/alice.jpg');
 assert.equal(s.sent[0].apns.fcmOptions.imageUrl,'https://example.com/alice.jpg');
});
test('transient token failure retries only failed devices',async()=>{
 const s=setup({'pushTokens/bob':{one:'good',two:'temporary'}});let first=true;
 s.setRespond(payload=>({responses:payload.tokens.map(t=>first&&t==='temporary'?{success:false,error:{code:'messaging/internal-error'}}:{success:true})}));
 await assert.rejects(s.dispatcher.deliver(s.event),/Retryable/);first=false;await s.dispatcher.deliver(s.event);assert.deepEqual(s.sent[1].tokens,['temporary']);
});
test('invalid token cleanup cannot remove a token refreshed during delivery',async()=>{
 const s=setup();s.setRespond(()=>{s.db.set('pushTokens/bob/phone','replacement');return {responses:[{success:false,error:{code:'messaging/registration-token-not-registered'}}]};});
 await s.dispatcher.deliver(s.event);assert.equal(s.db.value('pushTokens/bob/phone'),'replacement');
});
test('blocks, read/deleted messages, cancelled requests and nonfriends suppress alerts',async()=>{
 for(const data of [{'blocks/bob/alice':true},{'blocks/alice/bob':true},{'chatReads/bob/alice':1700000000000},{'messages/bob/alice/m1':null},{'deletionRequests/alice':{}}]){const s=setup(data);await s.dispatcher.deliver(s.event);assert.equal(s.sent.length,0);}
 const s=setup();await s.dispatcher.deliver({...s.event,type:'friend'});await s.dispatcher.deliver({...s.event,type:'emergency',startedAt:s.time});assert.equal(s.sent.length,0);
});
test('SOS alerts mutually accepted friends with sender name and map identity',async()=>{
 const s=setup({'friends/alice/bob':true,'friends/bob/alice':true,'emergencySignals/alice/startedAt':1700000000000});
 await s.dispatcher.deliver({...s.event,type:'emergency',id:String(s.time),startedAt:s.time});assert.equal(s.sent[0].notification.title,'SOS · Alice');assert.ok(s.sent[0].notification.body.includes('Alice needs help'));assert.equal(s.sent[0].data.senderId,'alice');assert.equal(s.sent[0].data.startedAt,String(s.time));assert.equal(s.sent[0].android.notification.channelId,'tuki-sos-v1');
});
test('first conversation and rapid same-timestamp messages are all delivered without mirror duplicates',async()=>{
 const s=setup({'messages/bob/alice/m1':null,'conversations':{}});const worker=createNotificationWorker({db:s.db,messaging:s.messaging,now:()=>s.time});await settle();
 s.db.set('conversations/bob',{alice:s.time});s.db.set('messages/bob/alice',{m1:{senderId:'alice',text:'First',createdAt:s.time},m2:{senderId:'alice',text:'Second',createdAt:s.time}});
 s.db.set('messages/bob/alice/m1',{senderId:'alice',text:'First',createdAt:s.time});s.db.emit('conversations','child_added',{alice:s.time},'bob');await settle();
 assert.equal(s.sent.length,2);assert.deepEqual(s.sent.map(p=>p.data.messageId).sort(),['m1','m2']);
 s.db.emit('messages/bob/alice','value',s.db.value('messages/bob/alice'));await settle();assert.equal(s.sent.length,2);worker.stop();
});
test('old history is not replayed; a cancelled then renewed friend request can alert again',async()=>{
 const s=setup({'conversations':{bob:{alice:1}},'messages/bob/alice/m1':{senderId:'alice',text:'Old',createdAt:1},'friendRequests':{bob:{alice:1}}});const worker=createNotificationWorker({db:s.db,messaging:s.messaging,now:()=>s.time});await settle();assert.equal(s.sent.length,0);
 s.db.emit('friendRequests/bob','value',{alice:s.time});await settle();assert.equal(s.sent.length,1);
 s.db.emit('friendRequests/bob','value',{});s.db.emit('friendRequests/bob','value',{alice:s.time+1});await settle();assert.equal(s.sent.length,2);worker.stop();
});

test('restart after 90 seconds catches unread messages, friend requests and active SOS exactly once',async()=>{
 for(const type of ['message','friend','emergency']){
  const at=1700000000000;
  const data=type==='message'?{conversations:{bob:{alice:at}}}:type==='friend'?{friendRequests:{bob:{alice:at}}}:{emergencySignals:{alice:{startedAt:at}},friends:{alice:{bob:true},bob:{alice:true}}};
  const s=setup(data),options={db:s.db,messaging:s.messaging,now:()=>at+90000};
  let worker=createNotificationWorker(options);await settle();assert.equal(s.sent.length,1,type);if(type==='emergency')assert.equal(s.sent[0].android.ttl,210000);worker.stop();
  worker=createNotificationWorker(options);await settle();assert.equal(s.sent.length,1,type+' must not replay');worker.stop();
 }
});

test('expired, read, blocked and cancelled events do not become alerts after restart',async()=>{
 const at=1700000000000;
 for(const extra of [{'chatReads/bob/alice':at},{'blocks/bob/alice':true},{'messages/bob/alice/m1':null},{'deletionRequests/bob':{status:'pending'}}]){
  const s=setup({conversations:{bob:{alice:at}},...extra});const worker=createNotificationWorker({db:s.db,messaging:s.messaging,now:()=>at+90000});await settle();assert.equal(s.sent.length,0);worker.stop();
 }
 const s=setup({conversations:{bob:{alice:at}},emergencySignals:{alice:{startedAt:at}},friends:{alice:{bob:true},bob:{alice:true}}});
 const worker=createNotificationWorker({db:s.db,messaging:s.messaging,now:()=>at+3600001});await settle();assert.equal(s.sent.length,0);worker.stop();
});

test('circle pushes carry circle routing and canonical text; same message ID in another circle is distinct',async()=>{
 const at=1700000000000,m={senderId:'alice',text:'Canonical circle message',createdAt:at};
 const s=setup({circles:{family:{name:'Family',members:{alice:true,bob:true}},team:{name:'Team',members:{alice:true,bob:true}}},circleMessages:{family:{same:m},team:{same:m}}});
 const worker=createNotificationWorker({db:s.db,messaging:s.messaging,now:()=>at+90000});await settle();assert.equal(s.sent.length,2);
 assert.deepEqual(s.sent.map(p=>p.data.circleId).sort(),['family','team']);assert.equal(s.sent[0].data.type,'circle');assert.equal(s.sent[0].data.ownerId,'bob');assert.equal(s.sent[0].data.threadId,s.sent[0].data.circleId);assert.equal(s.sent[0].notification.body,m.text);worker.stop();
 const again=createNotificationWorker({db:s.db,messaging:s.messaging,now:()=>at+90000});await settle();assert.equal(s.sent.length,2);again.stop();
});

test('circle push rechecks membership, read, hidden, blocks and sender authenticity',async()=>{
 const at=1700000000000,event={type:'circle',circleId:'family',recipient:'bob',sender:'alice',id:'m',createdAt:at,text:'Forged client text'};
 for(const extra of [
  {'circles/family/members/bob':null},{'circles/family/members/alice':null},
  {'circleReads/family/bob':at},{'circleHidden/bob/family/m':true},{'blocks/alice/bob':true},
  {'circleMessages/family/m':null},{'circleMessages/family/m/senderId':'mallory'},
  {'circleMessages/family/m/createdAt':at+1},{'deletionRequests/alice':{}}
 ]){
  const s=setup({'circles/family':{name:'Family',members:{alice:true,bob:true}},'circleMessages/family/m':{senderId:'alice',text:'Hello',createdAt:at},...extra});
  await s.dispatcher.deliver(event);assert.equal(s.sent.length,0,JSON.stringify(extra));
 }
});

test('durable retry survives worker loss without a conversation listener and stores no message body',async()=>{
 const {createNotificationQueue}=require('./notification-queue.cjs');const {eventKey}=require('./notification-policy.cjs');
 const s=setup();let at=s.time;
 const q=createNotificationQueue({db:s.db,deliver:async()=>{throw Error('FCM offline');},now:()=>at,retryMs:50000});
 await q.enqueue(s.event);await settle();const key=eventKey(s.event),saved=s.db.value('notificationPending/'+key);
 assert.ok(saved.event);assert.equal(saved.event.text,undefined);assert.equal(saved.event.recipient,'bob');q.stop();
 at+=60000;const next=createNotificationQueue({db:s.db,deliver:createNotificationDispatcher({db:s.db,messaging:s.messaging,now:()=>at}).deliver,now:()=>at});
 await settle();assert.equal(s.sent.length,1);assert.equal(s.db.value('notificationPending/'+key),null);next.stop();
});

test('retry after no token does not send a cancelled SOS or expired message',async()=>{
 const {createNotificationQueue}=require('./notification-queue.cjs'),{eventKey}=require('./notification-policy.cjs');
 for(const type of ['message','emergency']){
  const s=setup({'pushTokens/bob':null,'friends/alice/bob':true,'friends/bob/alice':true,'emergencySignals/alice/startedAt':1700000000000});let at=s.time;
  const event=type==='message'?s.event:{type,recipient:'bob',sender:'alice',id:String(at),startedAt:at};
  const dispatcher=createNotificationDispatcher({db:s.db,messaging:s.messaging,now:()=>at});
  const first=createNotificationQueue({db:s.db,deliver:dispatcher.deliver,now:()=>at,retryMs:50000});await first.enqueue(event);await settle();first.stop();
  s.db.set('pushTokens/bob',{phone:'token-bob'});if(type==='emergency')s.db.set('emergencySignals/alice/startedAt',null);
  at+=type==='emergency'?60000:3600001;
  const next=createNotificationQueue({db:s.db,deliver:dispatcher.deliver,now:()=>at});await settle();assert.equal(s.sent.length,0);assert.equal(s.db.value('notificationPending/'+eventKey(event)),null);next.stop();
 }
});

test('durable queue bounds concurrent dispatch and leaves deferred records on stop',async()=>{
 const {createNotificationQueue}=require('./notification-queue.cjs');const s=setup(),releases=[];let active=0,maximum=0;
 const q=createNotificationQueue({db:s.db,now:()=>s.time,deliver:()=>new Promise(resolve=>{active++;maximum=Math.max(maximum,active);releases.push(()=>{active--;resolve({status:'done',sent:1});});})});
 for(let i=0;i<10;i++)await q.enqueue({...s.event,id:'m'+i});await settle();assert.equal(maximum,4);assert.equal(releases.length,4);
 q.stop();for(const release of releases)release();await settle();assert.equal(releases.length,4);assert.ok(Object.keys(s.db.value('notificationPending')).length>=6);
});

test('authenticated wake supports Circle, friend and SOS references and rejects forged circle sender',async()=>{
 const at=1700000000000,s=setup({'circles/family':{members:{alice:true,bob:true}},'circleMessages/family/m':{senderId:'alice',text:'Hello',createdAt:at},'friendRequests/bob/alice':at,'emergencySignals/alice/startedAt':at,'friends/alice/bob':true});
 const events=[];const handler=require('./message-wake.cjs').createMessageWake({auth:{verifyIdToken:async(token,revoked)=>{assert.equal(revoked,true);return{uid:token};}},db:s.db,worker:{enqueue:async event=>events.push(event)},now:()=>at+90000});
 const call=async(body,uid='alice')=>{let status;return handler({body,get:()=> 'Bearer '+uid},{status(code){status=code;return this;},json(){return status;}});};
 assert.equal(await call({type:'circle',circleId:'family',messageId:'m'}),202);assert.equal(events[0].recipient,'bob');assert.equal(events[0].text,undefined);
 assert.equal(await call({type:'circle',circleId:'family',messageId:'m'},'mallory'),404);
 assert.equal(await call({type:'friend',recipient:'bob'}),202);assert.equal(events[1].type,'friend');
 assert.equal(await call({type:'emergency'}),202);assert.equal(events[2].type,'emergency');
 s.db.set('emergencySignals/alice/startedAt',null);assert.equal(await call({type:'emergency'}),404);
});
