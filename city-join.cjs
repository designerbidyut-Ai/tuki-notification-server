// Uses the existing free notification worker. All records here are admin-only.
// A device geocoder supplies a city, never the editable profile's "Lives in".
const {createHash}=require('node:crypto');
const {validKey}=require('./notification-policy.cjs');
const DAY=86400000;
const hash=value=>createHash('sha256').update(value).digest('hex');
function cityIdentity(input){
 const clean=value=>typeof value==='string'?value.normalize('NFKC').replace(/[\u0000-\u001f\u007f]/g,'').trim().replace(/\s+/g,' '):'';
 const city=clean(input?.city),region=clean(input?.region),country=clean(input?.country).toUpperCase();
 if(!city||city.length>100||region.length>100||!/^([A-Z]{2})$/.test(country))return null;
 return {city,region,country,cityKey:hash([country,region.toLowerCase(),city.toLowerCase()].join('|'))};
}
function pointValid(p,now,maxAge=120000){return p&&Number.isFinite(p.latitude)&&Math.abs(p.latitude)<=90&&Number.isFinite(p.longitude)&&Math.abs(p.longitude)<=180&&Number.isFinite(p.timestamp)&&p.timestamp<=now+60000&&p.timestamp>=now-maxAge;}
function distance(a,b){const rad=Math.PI/180,x=(b.latitude-a.latitude)*rad,y=(b.longitude-a.longitude)*rad;return 12742000*Math.asin(Math.min(1,Math.sqrt(Math.sin(x/2)**2+Math.cos(a.latitude*rad)*Math.cos(b.latitude*rad)*Math.sin(y/2)**2)));}
// Background GPS can keep a city eligible without reopening Tuki. Reject stale
// fixes and travel far from the geocoded area, rather than claiming an old city.
function memberCurrent(member,gps,now){return Number.isFinite(member?.updatedAt)&&member.updatedAt>0&&member.updatedAt<=now+60000&&pointValid(gps,now,DAY)&&Number.isFinite(member.latitude)&&Number.isFinite(member.longitude)&&distance(member,gps)<=10000;}

function createCityMembership({auth,db,now=Date.now}){
 const rates=new Map();
 return async(req,res)=>{
  const bearer=/^Bearer (\S+)$/.exec(req.get('authorization')||'');
  if(!bearer)return res.status(401).json({error:'Authentication required'});
  const body=req.body||{},city=cityIdentity(body),at=now();
  if(!city||!pointValid(body,at)||!validKey(body.deviceId)||typeof body.enabled!=='boolean')return res.status(400).json({error:'Invalid city registration'});
  let uid;
  try{const user=await auth.verifyIdToken(bearer[1],true);if(user.firebase?.sign_in_provider!=='google.com')return res.status(403).json({error:'Google sign-in required'});uid=user.uid;}catch{return res.status(401).json({error:'Invalid authentication'});}
  if(!validKey(uid))return res.status(401).json({error:'Invalid account'});
  if(rates.get(uid)>at-60000)return res.status(429).json({error:'Please retry later'});
  rates.delete(uid);rates.set(uid,at);while(rates.size>2048)rates.delete(rates.keys().next().value);
  try{
   const [visibility,gps,deletion,profile,token,user]=await Promise.all([
    db.ref('visibility/'+uid).get(),db.ref('publicLocations/'+uid).get(),db.ref('deletionRequests/'+uid).get(),
    db.ref('profiles/'+uid+'/uniqueId').get(),db.ref('pushTokens/'+uid+'/'+body.deviceId).get(),auth.getUser(uid)
   ]);
   if(deletion.exists()||user.disabled||visibility.val()!=='public'||!profile.val())return res.status(403).json({error:'Public sharing required'});
   if(!pointValid(gps.val(),at)||distance(gps.val(),body)>1000)return res.status(409).json({error:'Waiting for current shared GPS'});
   const deviceReady=body.enabled&&typeof token.val()==='string'&&!!token.val();
   // No raw location history or FCM token is copied into these records.
   await db.ref().update({['cityMembers/'+uid]:{...city,latitude:Math.round(body.latitude*100)/100,longitude:Math.round(body.longitude*100)/100,updatedAt:at},
    ['cityJoinDevices/'+uid+'/'+body.deviceId]:deviceReady?hash(token.val()):null});
   const signupAt=Date.parse(user.metadata?.creationTime);
   // Auth creation time prevents old profiles, reinstall and login from being
   // announced as new accounts when this feature is first deployed.
   if(Number.isFinite(signupAt)&&signupAt<=at+60000&&signupAt>=at-DAY){
    await db.ref('cityJoinEvents/'+uid).transaction(current=>current?undefined:{cityKey:city.cityKey,city:city.city,createdAt:at,signupAt,state:'pending',cursor:''});
   }
   return res.status(200).json({status:'registered',deviceReady:!!deviceReady});
  }catch{return res.status(503).json({error:'Please retry later'});}
 };
}

// Recheck both people's privacy and current city when queued work is delivered.
async function cityEventAllowed(db,event,now){
 const [join,sender,recipient,senderVisibility,recipientVisibility,senderGps,recipientGps]=await Promise.all([
  db.ref('cityJoinEvents/'+event.sender).get(),db.ref('cityMembers/'+event.sender).get(),db.ref('cityMembers/'+event.recipient).get(),
  db.ref('visibility/'+event.sender).get(),db.ref('visibility/'+event.recipient).get(),db.ref('publicLocations/'+event.sender).get(),db.ref('publicLocations/'+event.recipient).get()
 ]);
 const j=join.val(),a=sender.val(),b=recipient.val();
 if(!j||event.id!=='joined'||j.createdAt!==event.createdAt||j.createdAt<now-DAY||!Number.isFinite(j.signupAt)||j.signupAt<j.createdAt-DAY||j.signupAt>j.createdAt+60000||senderVisibility.val()!=='public'||recipientVisibility.val()!=='public'||!a||!b||j.cityKey!==a.cityKey||a.cityKey!==b.cityKey||!memberCurrent(a,senderGps.val(),now)||!memberCurrent(b,recipientGps.val(),now)||distance(senderGps.val(),recipientGps.val())>75000)return false;
 event.city=String(j.city).slice(0,100);return true;
}
async function reserveCityBudget(db,uid,key,now){
 const day=Math.floor(now/DAY);
 const result=await db.ref('cityJoinBudget/'+uid).transaction(old=>{
  const events=old?.day===day?old.events||{}:{};
  if(events[key])return {day,events};
  if(Object.keys(events).length>=3)return;
  return {day,events:{...events,[key]:true}};
 });
 return result.committed;
}

// One canonical event per new account, paged fan-out, persisted cursor, and
// durable per-device acknowledgements in the existing notification dispatcher.
async function fanoutCityJoin({db,worker,uid,now=Date.now,active=()=>true}){
 const record=db.ref('cityJoinEvents/'+uid);let event=(await record.get()).val();
 if(!event||event.state==='done'||event.createdAt<now()-DAY)return;
 while(active()){
  const rows=await db.ref('cityMembers').orderByChild('cityKey').startAt(event.cityKey,event.cursor||undefined).endAt(event.cityKey).limitToFirst(101).get();
  const entries=[];rows.forEach(row=>{const member=row.val();if(row.key!==event.cursor&&member.cityKey===event.cityKey)entries.push([row.key,member]);});
  // RTDB may first call a transaction with null even when the server record
  // exists. Propose null (a no-op for a deleted event) so a live event retries
  // with its server value; aborting on that initial null would lose later pages.
  if(!entries.length){await record.transaction(current=>current?{...current,state:'done'}:null);return;}
  for(const [recipient,member]of entries){
   if(!active())return;
   if(recipient!==uid&&Number.isFinite(member.updatedAt))await worker.enqueue({type:'city_join',recipient,sender:uid,id:'joined',createdAt:event.createdAt});
  }
  event.cursor=entries.at(-1)[0];const progress=await record.transaction(current=>current?{...current,cursor:event.cursor}:null);if(!progress.committed||!progress.snapshot.exists())return;
 }
}
function startCityJoins({db,worker,now=Date.now,onError=()=>{}}){
 let closed=false;const running=new Map();
 function schedule(uid){
  if(closed||running.has(uid))return;
  running.set(uid,null);
  void fanoutCityJoin({db,worker,uid,now,active:()=>!closed}).then(()=>running.delete(uid)).catch(e=>{
   onError(e);if(closed){running.delete(uid);return;}
   const timer=setTimeout(()=>{running.delete(uid);schedule(uid);},30000);timer.unref?.();running.set(uid,timer);
  });
 }
 const ref=db.ref('cityJoinEvents').orderByChild('createdAt').startAt(now()-DAY);
 const changed=snapshot=>{if(snapshot.val()?.state==='pending')schedule(snapshot.key);};
 ref.on('child_added',changed,onError);ref.on('child_changed',changed,onError);
 return()=>{closed=true;ref.off('child_added',changed);ref.off('child_changed',changed);for(const timer of running.values())if(timer)clearTimeout(timer);running.clear();};
}
module.exports={cityIdentity,pointValid,distance,memberCurrent,createCityMembership,cityEventAllowed,reserveCityBudget,fanoutCityJoin,startCityJoins};
