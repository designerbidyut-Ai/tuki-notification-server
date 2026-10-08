// Durable per-owner state avoids downloading all followers on every change.
function startFollowersSync({db,onSynced=()=>{},onError=()=>{}}){
 const queues=new Map();let closed=false,legacyPromise;const source=db.ref('following');
 function legacy(){
  if(!legacyPromise)legacyPromise=db.ref('followers').get().then(snapshot=>{
   const byOwner=new Map();
   for(const[peer,people]of Object.entries(snapshot.val()||{}))for(const[uid,enabled]of Object.entries(people||{}))if(enabled){let peers=byOwner.get(uid);if(!peers){peers=new Set();byOwner.set(uid,peers);}peers.add(peer);}
   return byOwner;
  }).catch(error=>{legacyPromise=undefined;throw error;});
  return legacyPromise;
 }
 const sync=snapshot=>{
  const uid=snapshot.key;if(!uid||closed)return;
  const previous=queues.get(uid)||Promise.resolve();
  const work=previous.then(async()=>{
   if(closed)return;
   const [following,state]=await Promise.all([db.ref('following/'+uid).get(),db.ref('followerSyncState/'+uid).get()]);
   if(closed)return;
   const peers=following.val()||{},saved=state.val();
   const old=saved?.version===1?new Set(Object.keys(saved.peers||{})):new Set((await legacy()).get(uid)||[]);
   if(closed)return;
   for(const peer of old)if(!peers[peer]){await db.ref(`followers/${peer}/${uid}`).remove();if(closed)return;}
   for(const peer of Object.keys(peers))if(!old.has(peer)){await db.ref(`followers/${peer}/${uid}`).set(true);onSynced();if(closed)return;}
   // Interrupted writes remain retryable until all mirror operations finish.
   await db.ref('followerSyncState/'+uid).set({version:1,peers});
   if(legacyPromise)void legacyPromise.then(index=>index.delete(uid));
  }).catch(onError).finally(()=>{if(queues.get(uid)===work)queues.delete(uid);});queues.set(uid,work);
 };
 for(const event of ['child_added','child_changed','child_removed'])source.on(event,sync,onError);
 return ()=>{closed=true;for(const event of ['child_added','child_changed','child_removed'])source.off(event,sync);};
}
module.exports={startFollowersSync};
