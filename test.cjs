const {test}=require('node:test'),assert=require('node:assert/strict');
const {createNotificationDispatcher,createNotificationWorker}=require('./notification-core.cjs');
const clone=v=>v==null?v:JSON.parse(JSON.stringify(v));
function fakeDatabase(initial={}){
 const data=new Map(Object.entries(initial)),listeners=new Map();
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
  child:s=>ref(path+'/'+s),get:async()=>snap(path,value(path)),limitToLast:()=>ref(path),set:async val=>data.set(path,val),remove:async()=>data.set(path,null),
  update:async fields=>{for(const[k,v]of Object.entries(fields))data.set(path+'/'+k,v);},
  transaction:async fn=>{const next=fn(value(path));if(next===undefined)return {committed:false};data.set(path,next);return {committed:true,snapshot:snap(path,next)};},
  on(event,callback){const key=path+':'+event;const list=listeners.get(key)||[];list.push(callback);listeners.set(key,list);queueMicrotask(()=>{if(!list.includes(callback))return;const val=value(path);if(event==='value')callback(snap(path,val));else if(event==='child_added')for(const[k,v]of Object.entries(val||{}))callback(snap(path,v,k));});},
  off(event,callback){const list=listeners.get(path+':'+event)||[];const i=list.indexOf(callback);if(i>=0)list.splice(i,1);},
 };}
 return {ref,value,set:(path,val)=>data.set(path,val),emit(path,event,val,key){if(event==='value')data.set(path,val);for(const fn of listeners.get(path+':'+event)||[])fn(snap(path,val,key));}};
}
const settle=async()=>{for(let i=0;i<20;i++)await new Promise(setImmediate);};
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
