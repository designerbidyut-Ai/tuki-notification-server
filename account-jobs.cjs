function startAccountJobs({db,auth,deleteAccount,deleteMedia,onError=()=>{}}){
 let closed=false,busy=false;const pending=new Set();
 async function drain(){if(closed||busy)return;busy=true;try{for(const uid of [...pending]){if(closed)break;pending.delete(uid);const job=(await db.ref('deletionRequests/'+uid).get()).val();if(!job||!['pending','processing'].includes(job.status))continue;try{await deleteAccount(db,auth,uid,{deleteMedia});}catch(e){pending.add(uid);onError(e);break;}}}finally{busy=false;}}
 const query=db.ref('deletionRequests').orderByChild('status').startAt('pending').endAt('processing').limitToFirst(5);
 const receive=s=>{if(closed)return;pending.add(s.key);void drain().catch(onError);};
 query.on('child_added',receive,onError);query.on('child_changed',receive,onError);
 const timer=setInterval(()=>void drain().catch(onError),30000);timer.unref?.();
 return()=>{closed=true;clearInterval(timer);query.off('child_added',receive);query.off('child_changed',receive);pending.clear();};
}
function createDeletionRequest({db,auth,now=Date.now}){return async(req,res)=>{
 const bearer=/^Bearer (\S+)$/.exec(req.get('authorization')||'');if(!bearer)return res.status(401).json({error:'Sign in again.'});
 let token;try{token=await auth.verifyIdToken(bearer[1],true);if(!token.uid||!token.auth_time||now()/1000-token.auth_time>300)throw Error();}catch{return res.status(401).json({error:'Sign in again to confirm deletion.'});}
 try{const job=db.ref('deletionRequests/'+token.uid);const previous=(await job.get()).val();if(!previous)await job.set({status:'pending',requestedAt:now()});return res.status(202).json({status:previous?.status||'pending'});}catch{return res.status(503).json({error:'Could not request deletion. Please retry.'});}
};}
module.exports={startAccountJobs,createDeletionRequest};
