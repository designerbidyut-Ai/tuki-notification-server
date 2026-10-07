// Wake a sleeping free host and recover only the authenticated sender's message.
// The request contains IDs; message content and recipient eligibility come from Firebase.
function createMessageWake({auth,db,worker,now=Date.now}){
 const validKey=value=>typeof value==='string'&&value.length>0&&value.length<=128&&!/[.#$\[\]/\u0000-\u0020/]/.test(value);
 return async(req,res)=>{
  const bearer=/^Bearer (\S+)$/.exec(req.get('authorization')||'');
  if(!bearer)return res.status(401).json({error:'Authentication required'});
  const {recipient,messageId}=req.body||{};
  if(!validKey(recipient)||!validKey(messageId))return res.status(400).json({error:'Invalid message reference'});
  let sender;
  try{sender=(await auth.verifyIdToken(bearer[1])).uid;}catch{return res.status(401).json({error:'Invalid authentication'});}
  if(!validKey(sender)||sender===recipient)return res.status(400).json({error:'Invalid recipient'});
  try{
   const message=(await db.ref(`messages/${recipient}/${sender}/${messageId}`).get()).val();
   if(!message||message.senderId!==sender||message.deleted||!Number.isFinite(message.createdAt)||message.createdAt<now()-300000||message.createdAt>now()+60000)return res.status(404).json({error:'Recent message not found'});
   worker.enqueue({type:'message',recipient,sender,id:messageId,text:message.text,createdAt:message.createdAt});
   return res.status(202).json({status:'queued'});
  }catch{return res.status(503).json({error:'Please retry'});}
 };
}
module.exports={createMessageWake};
