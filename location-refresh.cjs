const {randomUUID}=require('node:crypto');
const validId=value=>typeof value==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(value);
// Existing notification worker, existing FCM tokens. No location is accepted
// from the viewer or returned here; only the owner's device publishes GPS.
function createLocationRefresh({auth,db,messaging,now=Date.now}){
 const read=async path=>(await db.ref(path).get()).val();
 async function allowed(viewer,owner,circle){
  const denied=await Promise.all([`blocks/${viewer}/${owner}`,`blocks/${owner}/${viewer}`,`deletionRequests/${viewer}`,`deletionRequests/${owner}`].map(read));
  if(denied.some(value=>value!==null&&value!==undefined))return false;
  if(circle){
   const values=await Promise.all([`circles/${circle}/members/${viewer}`,`circles/${circle}/members/${owner}`,`circleSharing/${circle}/${owner}`].map(read));
   return values.every(value=>value===true);
  }
  const [a,b,forward,reverse]=await Promise.all([`visibility/${viewer}`,`visibility/${owner}`,`friends/${viewer}/${owner}`,`friends/${owner}/${viewer}`].map(read));
  const mutual=forward!=null&&reverse!=null;
  return (a==='public'||a==='friends'&&mutual)&&(b==='public'||b==='friends'&&mutual);
 }
 return async(req,res)=>{
  res.set?.('Cache-Control','no-store');
  const bearer=/^Bearer (\S+)$/.exec(req.get('authorization')||'');
  if(!bearer)return res.status(401).json({error:'Authentication required'});
  const {recipient,circleId}=req.body||{};
  if(!validId(recipient)||circleId!==undefined&&!validId(circleId))return res.status(400).json({error:'Invalid person'});
  let viewer;try{viewer=(await auth.verifyIdToken(bearer[1],true)).uid;}catch{return res.status(401).json({error:'Invalid authentication'});}
  if(!validId(viewer)||viewer===recipient)return res.status(400).json({error:'Invalid person'});
  try{
   if(!await allowed(viewer,recipient,circleId))return res.status(403).json({error:'Location is not shared'});
   const capability=await read('trackingDiagnostics/'+recipient+'/native');
   if(capability?.schema!==1||capability.source!=='native'||!/^android-build-[1-9][0-9]*$/.test(capability.runtime||''))return res.status(409).json({error:'Phone update required'});
   const devices=await read('pushTokens/'+recipient);
   const tokens=[...new Set(Object.values(devices||{}).filter(token=>typeof token==='string'&&token.length>0))].slice(0,20);
   if(!tokens.length)return res.status(409).json({error:'Phone unavailable'});
   // Durable per-viewer and per-phone limits also apply across worker restarts.
   const requestedAt=now();
   for(const [key,delay]of [['viewer/'+viewer,10000],['phone/'+recipient,30000]]){
    const lease=await db.ref('locationRefreshLimits/'+key).transaction(previous=>typeof previous==='number'&&previous>requestedAt-delay?undefined:requestedAt);
    if(!lease.committed)return res.status(429).json({error:'Refresh already requested'});
   }
   if(!await allowed(viewer,recipient,circleId))return res.status(403).json({error:'Location is not shared'});
   const requestId=randomUUID(),expiresAt=now()+45000;
   const result=await messaging.sendEachForMulticast({tokens,data:{type:'location-refresh',requestId,requesterId:viewer,recipientId:recipient,requestedAt:String(now()),expiresAt:String(expiresAt)},android:{priority:'high',ttl:45000,collapseKey:'tuki-location-refresh'},apns:{headers:{'apns-push-type':'background','apns-priority':'5','apns-expiration':String(Math.floor(expiresAt/1000))},payload:{aps:{contentAvailable:true}}}});
   if(!result.responses.some(response=>response.success))return res.status(409).json({error:'Phone unavailable'});
   return res.status(202).json({status:'requested',requestId,expiresAt});
  }catch{return res.status(503).json({error:'Please retry'});}
 };
}
module.exports={createLocationRefresh};
