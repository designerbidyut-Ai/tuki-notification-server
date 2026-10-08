const {createHash}=require('node:crypto');
const hash=value=>createHash('sha256').update(value).digest('hex');
function createCloudinaryMedia({cloudName,apiKey,apiSecret,fetchImpl=fetch}){
 if(!/^[a-z0-9_-]+$/i.test(cloudName||'')||!apiKey||!apiSecret)return null;
 const request=async(action,form)=>{
  const response=await fetchImpl('https://api.cloudinary.com/v1_1/'+cloudName+'/image/'+action,{method:'POST',headers:{Authorization:'Basic '+Buffer.from(apiKey+':'+apiSecret).toString('base64')},body:form,signal:AbortSignal.timeout(30000)});
  if(!response.ok)throw Error('Photo storage unavailable.');
  return response.json();
 };
 return {async upload({image,kind,publicId}){
  const form=new FormData();
  for(const[key,value]of Object.entries({file:image,public_id:publicId,overwrite:'false',unique_filename:'false',format:'jpg',transformation:kind==='avatar'?'c_fill,w_192,h_192,q_75':'c_limit,w_640,h_240,q_75'}))form.set(key,value);
  const result=await request('upload',form);
  if(result.public_id!==publicId||result.format!=='jpg'||!Number.isFinite(result.width)||!Number.isFinite(result.height)||result.width<1||result.height<1||result.width>(kind==='avatar'?192:640)||result.height>(kind==='avatar'?192:240))throw Error('Invalid photo storage response.');
  const url=new URL(result.secure_url);
  if(url.protocol!=='https:'||url.hostname!=='res.cloudinary.com'||!url.pathname.startsWith('/'+cloudName+'/image/upload/'))throw Error('Invalid photo URL.');
  return {url:url.href,publicId};
 },async destroy(publicId){
  if(!/^tuki\/[a-f0-9]{32}\/(avatar|cover)\/[a-f0-9]{64}$/.test(publicId))throw Error('Invalid managed photo.');
  const form=new FormData();form.set('public_id',publicId);form.set('invalidate','true');
  const result=await request('destroy',form);if(!['ok','not found'].includes(result.result))throw Error('Could not remove photo.');
 }};
}
function createProfileMedia({auth,db,storage,now=Date.now}){
 return async(req,res)=>{
  if(!storage)return res.status(503).json({error:'Photo storage is not configured.'});
  const bearer=/^Bearer (\S+)$/.exec(req.get('authorization')||'');
  if(!bearer)return res.status(401).json({error:'Sign in again.'});
  let uid;try{uid=(await auth.verifyIdToken(bearer[1],true)).uid;if(typeof uid!=='string'||uid.length>128||/[.#$\[\]/\u0000-\u0020]/.test(uid))throw Error();}catch{return res.status(401).json({error:'Sign in again.'});}
  const {kind,image}=req.body||{};
  const limit=kind==='avatar'?100000:kind==='cover'?300000:0;
  if(!limit||typeof image!=='string'||image.length>limit||!/^data:image\/jpeg;base64,[A-Za-z0-9+/]+={0,2}$/.test(image))return res.status(400).json({error:'Choose a valid JPEG photo.'});
  const bytes=Buffer.from(image.slice(image.indexOf(',')+1),'base64');
  if(bytes.length<4||bytes[0]!==255||bytes[1]!==216||bytes.at(-2)!==255||bytes.at(-1)!==217)return res.status(400).json({error:'Choose a valid JPEG photo.'});
  try{
   if((await db.ref('deletionRequests/'+uid).get()).exists())return res.status(403).json({error:'Account is unavailable.'});
   const digest=hash(bytes),record=db.ref(`mediaAssets/${uid}/${kind}/${digest}`);
   const previous=(await record.get()).val();if(previous?.url)return res.status(200).json({url:previous.url});
   const day=Math.floor(now()/86400000),rate=db.ref('mediaUploadRate/'+uid);
   const claim=await rate.transaction(old=>{const count=old?.day===day?old.count||0:0;return count>=10?undefined:{day,count:count+1};});
   if(!claim.committed)return res.status(429).json({error:'Photo upload limit reached. Try again tomorrow.'});
   const month=new Date(now()).toISOString().slice(0,7);
   const budget=await db.ref('mediaUploadBudget/'+month).transaction(count=>(count||0)>=5000?undefined:(count||0)+1);
   if(!budget.committed)return res.status(429).json({error:'Free photo quota reached. Please try again later.'});
   // Fixed owner/content keys make retry uploads idempotent and unguessable.
   const publicId=`tuki/${hash(uid).slice(0,32)}/${kind}/${digest}`;
   // Reserve cleanup metadata before uploading; a failed network/database response
   // must not leave an external asset with no owner record.
   const reservation=await record.transaction(old=>old||{publicId,createdAt:now(),status:'pending'});
   if(reservation.snapshot.val()?.url)return res.status(200).json({url:reservation.snapshot.val().url});
   const result=await storage.upload({image,kind,publicId});
   if((await db.ref('deletionRequests/'+uid).get()).exists())throw Error('Account became unavailable.');
   await record.set({...result,createdAt:now()});return res.status(200).json({url:result.url});
  }catch{return res.status(503).json({error:'Could not save the photo. Please retry.'});}
 };
}
async function pruneProfileMedia({db,storage,uid,now=Date.now,deleteAll=false}){
 if(!storage)return;
 if(deleteAll&&!(await db.ref('deletionRequests/'+uid).get()).exists())throw Error('Account deletion was not requested.');
 const records=(await db.ref('mediaAssets/'+uid).get()).val()||{};
 const active=deleteAll?[]:await Promise.all(['avatarUrl','coverUrl'].map(key=>db.ref('profiles/'+uid+'/'+key).get().then(s=>s.val())));
 const prefix='tuki/'+hash(uid).slice(0,32)+'/';
 for(const[kind,assets]of Object.entries(records))for(const[digest,item]of Object.entries(assets||{})){
  if(!item||item.publicId!==prefix+kind+'/'+digest)continue;
  if(!deleteAll&&(active.includes(item.url)||!Number.isFinite(item.createdAt)||item.createdAt>now()-86400000))continue;
  await storage.destroy(item.publicId);
  await db.ref(`mediaAssets/${uid}/${kind}/${digest}`).remove();
 }
}
function startMediaSweep({db,storage,onError=()=>{},intervalMs=3600000}){
 if(!storage)return()=>{};
 let closed=false,busy=false;
 async function sweep(){
  if(closed||busy)return;busy=true;
  try{
   const cursor=(await db.ref('serverHousekeeping/mediaSweepCursor').get()).val();
   let query=db.ref('mediaAssets').orderByKey().limitToFirst(10);if(cursor)query=query.startAfter(cursor);
   const owners=Object.keys((await query.get()).val()||{});
   for(const uid of owners){if(closed)return;await pruneProfileMedia({db,storage,uid});if(!closed)await db.ref('serverHousekeeping/mediaSweepCursor').set(uid);}
   if(!closed&&!owners.length)await db.ref('serverHousekeeping/mediaSweepCursor').remove();
  }catch(error){onError(error);}finally{busy=false;}
 }
 const timer=setInterval(()=>void sweep(),intervalMs);timer.unref?.();
 return()=>{closed=true;clearInterval(timer);};
}
module.exports={createCloudinaryMedia,createProfileMedia,pruneProfileMedia,startMediaSweep};
