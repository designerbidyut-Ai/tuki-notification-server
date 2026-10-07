// Preserve the existing Render worker's follower mirror while repairing push delivery.
function startFollowersSync({db,onSynced=()=>{},onError=()=>{}}){
 const queues=new Map();let closed=false;const source=db.ref('following');
 const sync=snapshot=>{
  const uid=snapshot.key;if(!uid||closed)return;
  const previous=queues.get(uid)||Promise.resolve();
  const work=previous.then(async()=>{
   if(closed)return;
   const [following,followers]=await Promise.all([db.ref('following/'+uid).get(),db.ref('followers').get()]);
   if(closed)return;
   const peers=following.val()||{},mirrors=followers.val()||{};
   for(const[peer,people]of Object.entries(mirrors))if(people?.[uid]&&!peers[peer])await db.ref(`followers/${peer}/${uid}`).remove();
   for(const peer of Object.keys(peers))if(mirrors[peer]?.[uid]!==true){await db.ref(`followers/${peer}/${uid}`).set(true);onSynced();}
  }).catch(onError).finally(()=>{if(queues.get(uid)===work)queues.delete(uid);});queues.set(uid,work);
 };
 for(const event of ['child_added','child_changed','child_removed'])source.on(event,sync,onError);
 return ()=>{closed=true;for(const event of ['child_added','child_changed','child_removed'])source.off(event,sync);};
}
module.exports={startFollowersSync};
