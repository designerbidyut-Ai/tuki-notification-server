// Wake a sleeping free host and recover only the authenticated sender's message.
// The request contains IDs; message content and recipient eligibility come from Firebase.
function createMessageWake({auth,db,worker,now=Date.now}){
 const rates=new Map();
 const validKey=value=>typeof value==='string'&&value.length>0&&value.length<=128&&!/[.#$\[\]/\u0000-\u0020/]/.test(value);
 return async(req,res)=>{
  const bearer=/^Bearer (\S+)$/.exec(req.get('authorization')||'');
  if(!bearer)return res.status(401).json({error:'Authentication required'});
  const {recipient,messageId,circleId,type='message'}=req.body||{};
  if(!['message','circle','friend','emergency'].includes(type)||(type==='message'&&(!validKey(recipient)||!validKey(messageId)))||(type==='friend'&&!validKey(recipient))||(type==='circle'&&(!validKey(circleId)||!validKey(messageId))))return res.status(400).json({error:'Invalid event reference'});
  let sender;
  try{sender=(await auth.verifyIdToken(bearer[1],true)).uid;}catch{return res.status(401).json({error:'Invalid authentication'});}
  if(!validKey(sender)||sender===recipient)return res.status(400).json({error:'Invalid recipient'});
  const slot=Math.floor(now()/60000),previous=rates.get(sender),count=previous?.slot===slot?previous.count+1:1;
  rates.delete(sender);rates.set(sender,{slot,count});if(rates.size>1024)rates.delete(rates.keys().next().value);
  if(count>60)return res.status(429).json({error:'Please wait before retrying'});
  try{
   if((await db.ref('deletionRequests/'+sender).get()).exists())return res.status(403).json({error:'Account unavailable'});
   const events=[];
   if(type==='message'||type==='circle'){
    const message=(await db.ref(type==='message'?`messages/${recipient}/${sender}/${messageId}`:`circleMessages/${circleId}/${messageId}`).get()).val();
    if(!message||message.senderId!==sender||message.deleted||!Number.isFinite(message.createdAt)||message.createdAt<now()-300000||message.createdAt>now()+60000)return res.status(404).json({error:'Recent message not found'});
    if(type==='message')events.push({type,recipient,sender,id:messageId,createdAt:message.createdAt});
    else{
     const members=(await db.ref('circles/'+circleId+'/members').get()).val()||{};
     if(members[sender]!==true)return res.status(403).json({error:'Circle unavailable'});
     for(const peer of Object.keys(members).slice(0,30))if(peer!==sender&&members[peer]===true)events.push({type,circleId,recipient:peer,sender,id:messageId,createdAt:message.createdAt});
    }
   }else if(type==='friend'){
    const at=(await db.ref(`friendRequests/${recipient}/${sender}`).get()).val();
    if(!Number.isFinite(at)||at<now()-300000||at>now()+60000)return res.status(404).json({error:'Recent request not found'});
    events.push({type,recipient,sender,id:String(at),createdAt:at});
   }else{
    const at=(await db.ref('emergencySignals/'+sender+'/startedAt').get()).val();
    if(!Number.isFinite(at)||at<now()-300000||at>now()+60000)return res.status(404).json({error:'Active recent signal not found'});
    const friends=(await db.ref('friends/'+sender).get()).val()||{};
    for(const peer of Object.keys(friends))events.push({type,recipient:peer,sender,id:String(at),startedAt:at});
   }
   await Promise.all(events.map(event=>worker.enqueue(event)));
   return res.status(202).json({status:'queued'});
  }catch{return res.status(503).json({error:'Please retry'});}
 };
}
module.exports={createMessageWake};
