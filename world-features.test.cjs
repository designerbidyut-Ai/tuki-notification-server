const {test}=require('node:test'),assert=require('node:assert/strict');
const {createHash}=require('node:crypto');
const {createThreadSummaries}=require('./thread-summaries.cjs');
const {createProfileMedia,createCloudinaryMedia,pruneProfileMedia}=require('./profile-media.cjs');
const clone=v=>v==null?v:structuredClone(v);
function database(initial={}){
 const data=clone(initial),listeners=new Map(),reads=[],writes=[];
 const read=path=>path.split('/').filter(Boolean).reduce((v,k)=>v?.[k],data)??null;
 function write(path,value){const bits=path.split('/').filter(Boolean);let node=data;for(const bit of bits.slice(0,-1))node=node[bit]??={};if(value===null)delete node[bits.at(-1)];else node[bits.at(-1)]=clone(value);writes.push(path);}
 const snapshot=(path,value,key=path.split('/').at(-1))=>({key,val:()=>clone(value),exists:()=>value!=null});
 function ref(path=''){
  let order='',end=Infinity,start=-Infinity,after=null,limit=Infinity,tail=false;
  const result={child:child=>ref(path+'/'+child),orderByKey:()=>{order='$key';return result;},orderByChild:key=>{order=key;return result;},endAt:value=>{end=value;return result;},startAt:value=>{start=value;return result;},startAfter:value=>{after=value;return result;},limitToFirst:value=>{limit=value;return result;},limitToLast:value=>{limit=value;tail=true;return result;},
   get:async()=>{reads.push(path);let value=read(path);if(order){let entries=Object.entries(value||{}).filter(([k,v])=>(!after||k>after)&&(order==='$key'||(v[order]<=end&&v[order]>=start))).sort(([a,v],[b,w])=>order==='$key'?a.localeCompare(b):(v[order]-w[order]||a.localeCompare(b)));value=Object.fromEntries(tail?entries.slice(-limit):entries.slice(0,limit));}return snapshot(path,value);},
   set:async value=>write(path,value),remove:async()=>write(path,null),update:async fields=>{for(const[k,v]of Object.entries(fields))write([path,k].filter(Boolean).join('/'),v);},
   transaction:async fn=>{const next=fn(clone(read(path)));if(next!==undefined)write(path,next);return {committed:next!==undefined,snapshot:snapshot(path,read(path))};},
   on(event,fn){const id=path+':'+event;const list=listeners.get(id)||new Set();list.add(fn);listeners.set(id,list);queueMicrotask(()=>{if(!list.has(fn))return;const value=read(path);if(event==='value')fn(snapshot(path,value));else if(event==='child_added')for(const[k,v]of Object.entries(value||{}))fn(snapshot(path,v,k));});},
   off(event,fn){listeners.get(path+':'+event)?.delete(fn);},
  };return result;
 }
 return {ref,read,reads,writes,write,emit(path,event,value,key){if(event==='value')write(path,value);for(const fn of listeners.get(path+':'+event)||[])fn(snapshot(path,value,key));},listenerCount:()=>[...listeners.values()].reduce((n,s)=>n+s.size,0)};
}
const tick=async()=>{for(let n=0;n<12;n++)await new Promise(setImmediate);};
test('report inbox pagination retains more than twenty reports from one reporter and includes canonical context',async()=>{const reports=Object.fromEntries(Array.from({length:25},(_,i)=>['r'+String(i).padStart(3,'0'),{targetId:'bob',messageId:'m',createdAt:i+1,details:'test'}]));const db=database({reports:{alice:reports},messages:{alice:{bob:{m:{senderId:'bob',text:'context',createdAt:1}}}}});const {startReportIndex,createModeration}=require('./moderation.cjs'),stop=startReportIndex({db,onError:e=>{throw e;}});await tick();let now=1700000000000;const admin=createModeration({db,email:'owner@example.test',now:()=>now,auth:{verifyIdToken:async()=>({uid:'owner',email:'owner@example.test',email_verified:true,firebase:{sign_in_provider:'google.com'}})}});const first=response();await admin.list({get:()=>'Bearer google',query:{}},first);assert.equal(first.body.rows.length,20);assert.equal(first.body.rows[0].message.text,'context');now+=1000;const second=response();await admin.list({get:()=>'Bearer google',query:{cursor:first.body.nextCursor}},second);assert.equal(second.body.rows.length,5);assert.equal(second.body.nextCursor,null);db.emit('reports','child_removed',null,'alice');await tick();assert.equal(Object.keys(db.read('reportInbox')||{}).length,0);stop();assert.equal(db.listenerCount(),0);});
test('account photo cleanup refuses completion when storage is unavailable',async()=>{await assert.rejects(pruneProfileMedia({db:database(),storage:null,uid:'alice',deleteAll:true}),/unavailable/);});
function response(){return{statusCode:200,status(n){this.statusCode=n;return this;},json(value){this.body=value;return this;}};}
test('deletion endpoint requires recent owner authentication and never accepts another body UID',async()=>{const {createDeletionRequest}=require('./account-jobs.cjs'),db=database(),now=1700000000000;const handler=createDeletionRequest({db,now:()=>now,auth:{verifyIdToken:async token=>{if(token==='invalid')throw Error();return{uid:'alice',auth_time:token==='old'?now/1000-600:now/1000};}}});for(const token of ['', 'invalid','old']){const res=response();await handler({get:()=>token?'Bearer '+token:'',body:{uid:'bob'}},res);assert.equal(res.statusCode,401);assert.equal(db.read('deletionRequests/alice'),null);}const res=response();await handler({get:()=>'Bearer fresh',body:{uid:'bob'}},res);assert.equal(res.statusCode,202);assert.equal(db.read('deletionRequests/alice/status'),'pending');assert.equal(db.read('deletionRequests/bob'),null);});
test('moderation refuses unverified/non-Google accounts and only removes the reported sender message',async()=>{const {createModeration}=require('./moderation.cjs');const db=database({reports:{alice:{report:{targetId:'bob',messageId:'m',createdAt:1,details:'test'}}},messages:{alice:{bob:{m:{senderId:'bob',text:'reported'}}},bob:{alice:{m:{senderId:'bob',text:'reported'}}},carol:{bob:{keep:{senderId:'bob',text:'keep'}}}}});let now=1700000000000;const moderation=createModeration({db,email:'owner@example.test',now:()=>now,auth:{verifyIdToken:async token=>({uid:'owner',email:'owner@example.test',email_verified:token!=='unverified',firebase:{sign_in_provider:token==='custom'?'custom':'google.com'}})}});for(const token of ['unverified','custom']){const res=response();await moderation.list({get:()=>'Bearer '+token,query:{}},res);assert.equal(res.statusCode,403);}const res=response();await moderation.review({get:()=>'Bearer google',body:{reporterId:'alice',id:'report',action:'remove_message',note:'confirmed'}},res);assert.equal(res.statusCode,200);assert.equal(db.read('messages/alice/bob/m'),null);assert.equal(db.read('messages/bob/alice/m'),null);assert.notEqual(db.read('messages/carol/bob/keep'),null);assert.equal(db.read('reportReviews/alice/report/action'),'remove_message');});
test('circle summary hides user-deleted content, follows read cursors and cleans revoked membership',async()=>{const db=database({circles:{g:{members:{alice:true,bob:true}}},circleMessages:{g:{old:{senderId:'bob',text:'hidden',createdAt:1},fresh:{senderId:'bob',text:'x'.repeat(2000),createdAt:2}}},circleHidden:{alice:{g:{old:true}}},circleReads:{g:{alice:1}}});const stop=require('./circle-summaries.cjs').startCircleSummaries({db,onError:e=>{throw e;}});await tick();assert.equal(db.read('circleSummaries/g/alice/latest/text').length,160);assert.equal(db.read('circleSummaries/g/alice/unreadCount'),1);assert.equal(db.read('circleIndexReady/g'),true);db.emit('circleReads/g','value',{alice:2});await tick();assert.equal(db.read('circleSummaries/g/alice/unreadCount'),0);db.write('circles/g',{members:{bob:true}});db.emit('circles','child_changed',{members:{bob:true}},'g');await tick();assert.equal(db.read('circleSummaries/g/alice'),null);stop();assert.equal(db.listenerCount(),0);});
test('geographic index derives the current point and cannot recreate a private/deleted record',async()=>{
 const time=1700000000000,db=database({publicLocations:{alice:{latitude:23.81,longitude:90.41,timestamp:time}}});
 const stop=require('./public-geo-index.cjs').startPublicGeoIndex({db,onError:e=>{throw e;}});await tick();
 assert.equal(db.read('publicLocations/alice/geoIndex'),'1138-2704:'+time);
 db.emit('publicLocations','child_changed',{latitude:23.81,longitude:90.41,timestamp:time},'alice');db.write('publicLocations/alice',null);await tick();assert.equal(db.read('publicLocations/alice'),null);
 stop();db.write('publicLocations/alice',{latitude:0,longitude:0,timestamp:time});db.emit('publicLocations','child_added',db.read('publicLocations/alice'),'alice');await tick();assert.equal(db.read('publicLocations/alice/geoIndex'),null);
});
test('compact summaries reuse supplied snapshots, update read counts and remove closed conversations',async()=>{
 const db=database({chatReads:{alice:{bob:1}}}),index=createThreadSummaries({db,onError:e=>{throw e;}});
 index.threads('alice',{bob:4});index.messages('alice','bob',{a:{senderId:'bob',text:'x'.repeat(2000),createdAt:2},b:{senderId:'alice',text:'Reply',createdAt:3},gone:{senderId:'bob',text:'Deleted',createdAt:4,deleted:true}});await tick();
 assert.equal(db.read('threadSummaries/alice/bob/latest/text'),'Reply');assert.equal(db.read('threadSummaries/alice/bob/latestIncoming/text').length,160);assert.equal(db.read('threadSummaries/alice/bob/unreadCount'),1);assert.equal(db.read('threadIndexReady/alice'),true);
 assert.ok(db.reads.every(path=>!path.startsWith('messages/')));
 db.emit('chatReads/alice','value',{bob:2});await tick();assert.equal(db.read('threadSummaries/alice/bob/unreadCount'),0);
 index.messages('alice','bob',{c:{senderId:'bob',text:'Late',createdAt:5}});index.threads('alice',{});await tick();assert.equal(db.read('threadSummaries/alice/bob'),null);
 index.stop();assert.equal(db.listenerCount(),0);index.threads('alice',{bob:6});await tick();assert.equal(db.listenerCount(),0);
});
test('popup feed preserves rapid message identities and rejects blocked, read, forged and stale events',async()=>{
 const time=1700000000000,db=database({messages:{bob:{alice:{one:{senderId:'alice',text:'One',createdAt:time},two:{senderId:'alice',text:'Two',createdAt:time}}}},messageInbox:{bob:{old:{createdAt:time-90000000}}}}),index=createThreadSummaries({db,now:()=>time});
 const event={type:'message',recipient:'bob',sender:'alice',id:'one',text:'Spoofed',createdAt:time};
 await index.incoming(event);await index.incoming({...event,id:'two'});await index.incoming(event);
 const rows=Object.values(db.read('messageInbox/bob'));assert.equal(rows.length,2);assert.deepEqual(rows.map(m=>m.text).sort(),['One','Two']);
 db.write('blocks/bob/alice',true);await index.incoming({...event,id:'blocked'});assert.equal(Object.keys(db.read('messageInbox/bob')).length,2);
 db.write('blocks/bob/alice',null);db.write('chatReads/bob/alice',time);await index.incoming({...event,id:'read'});await index.incoming({...event,id:'missing'});await index.incoming({...event,createdAt:1});assert.equal(Object.keys(db.read('messageInbox/bob')).length,2);index.stop();
});
const jpeg='data:image/jpeg;base64,'+Buffer.from([255,216,1,2,255,217]).toString('base64');
function mediaSetup(initial={}){
 const time=1700000000000,db=database(initial),uploads=[];
 const storage={upload:async options=>{uploads.push(options);return{url:'https://res.cloudinary.com/demo/image/upload/'+options.publicId+'.jpg',publicId:options.publicId};}};
 const auth={verifyIdToken:async(token,revoke)=>{assert.equal(revoke,true);if(token!=='valid')throw Error();return{uid:'alice'};}};
 const handler=createProfileMedia({auth,db,storage,now:()=>time});
 const call=async(body={kind:'avatar',image:jpeg},token='valid')=>{let status,result;await handler({body,get:()=>token?'Bearer '+token:''},{status(value){status=value;return this;},json(value){result=value;return this;}});return{status,result};};
 return {time,db,storage,uploads,call};
}
test('photo uploads require verified ownership, valid JPEG and reuse immutable assets on retries',async()=>{
 const s=mediaSetup();assert.equal((await s.call(undefined,'')).status,401);assert.equal((await s.call({kind:'avatar',image:'https://other.test/a'})).status,400);
 const first=await s.call();assert.equal(first.status,200);assert.equal((await s.call()).result.url,first.result.url);assert.equal(s.uploads.length,1);assert.ok(!s.uploads[0].publicId.includes('alice'));assert.equal(s.db.read('mediaUploadRate/alice/count'),1);
 s.db.write('deletionRequests/alice',{status:'pending'});assert.equal((await s.call()).status,403);
});
test('daily/project free upload caps and failed uploads keep cleanup metadata without storing base64',async()=>{
 const s=mediaSetup();s.db.write('mediaUploadRate/alice',{day:Math.floor(s.time/86400000),count:10});assert.equal((await s.call()).status,429);assert.equal(s.uploads.length,0);
 s.db.write('mediaUploadRate/alice',null);s.db.write('mediaUploadBudget/'+new Date(s.time).toISOString().slice(0,7),5000);assert.equal((await s.call()).status,429);
 s.db.write('mediaUploadBudget/'+new Date(s.time).toISOString().slice(0,7),0);s.storage.upload=async()=>{throw Error('offline');};assert.equal((await s.call()).status,503);
 const records=s.db.read('mediaAssets/alice/avatar');assert.equal(Object.values(records)[0].status,'pending');assert.ok(!JSON.stringify(records).includes('base64'));
});
test('Cloudinary requests keep credentials in backend authorization and enforce thumbnail bounds',async()=>{
 const calls=[];let result={public_id:'tuki/test',format:'jpg',width:192,height:192,secure_url:'https://res.cloudinary.com/demo/image/upload/tuki/test.jpg'};
 const provider=createCloudinaryMedia({cloudName:'demo',apiKey:'private-key',apiSecret:'private-secret',fetchImpl:async(url,options)=>{calls.push({url,options});return{ok:true,json:async()=>result};}});
 await provider.upload({image:jpeg,kind:'avatar',publicId:'tuki/test'});assert.equal(calls[0].options.body.get('overwrite'),'false');assert.equal(calls[0].options.body.get('transformation'),'c_fill,w_192,h_192,q_75');assert.ok(!calls[0].url.includes('private'));result.width=300;await assert.rejects(provider.upload({image:jpeg,kind:'avatar',publicId:'tuki/test'}),/Invalid/);
 assert.equal(createCloudinaryMedia({cloudName:'demo'}),null);
});
test('photo cleanup retains current and pending photos and checks owner/deletion request',async()=>{
 const now=1700000000000,uid='alice',prefix='tuki/'+createHash('sha256').update(uid).digest('hex').slice(0,32)+'/avatar/',old='a'.repeat(64),current='b'.repeat(64),recent='c'.repeat(64),destroyed=[];
 const db=database({profiles:{alice:{avatarUrl:'current'}},mediaAssets:{alice:{avatar:{[old]:{publicId:prefix+old,url:'old',createdAt:1},[current]:{publicId:prefix+current,url:'current',createdAt:1},[recent]:{publicId:prefix+recent,createdAt:now,status:'pending'}}}}});
 const storage={destroy:async id=>destroyed.push(id)};await pruneProfileMedia({db,storage,uid,now:()=>now});assert.deepEqual(destroyed,[prefix+old]);assert.ok(db.read('mediaAssets/alice/avatar/'+current));assert.ok(db.read('mediaAssets/alice/avatar/'+recent));
 await assert.rejects(pruneProfileMedia({db,storage,uid,deleteAll:true}),/not requested/);db.write('deletionRequests/alice',{status:'pending'});await pruneProfileMedia({db,storage,uid,deleteAll:true});assert.equal(Object.keys(db.read('mediaAssets/alice/avatar')).length,0);
});
