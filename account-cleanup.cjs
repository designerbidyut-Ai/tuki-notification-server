// Server-only account cleanup. Each owner is processed separately; progress and
// its deletions commit together, so retries resume without skipping records.
const ownCollections=['profiles','contacts','shares','incoming','liveLocations','publicLocations','visibility','presence','messages','conversations','notifications','pushTokens','shops','shopLikes','shopComments','shopMap','friendRequests','friendSent','friends','following','followers','sharingSettings','chatReads','chatDelivered','blocks','reportRate','reports','nearbyScans','privateProfileContacts','chatSaved','circleMemberships','circleHidden','emergencySignals','circleJoinClaims','followerSyncState'];
const mirrored=['contacts','shares','incoming','messages','conversations','friendRequests','friendSent','friends','following','followers','chatReads','chatDelivered','blocks','shopLikes'];
const scans=[...mirrored,'handles','notifications','shopComments','messageReactions','reports','circles','circleMessages','circleSharing','circleLocations','emergencyCircleSignals','circleInviteTokens'];
ownCollections.push('threadSummaries','threadIndexReady','messageInbox','mediaAssets','mediaUploadRate','reportReviews');
mirrored.push('threadSummaries');
scans.push('threadSummaries','messageInbox','circleSummaries','circleReads','reportInbox');
function removalFor(collection,owner,value,uid){
 const changes={},base=collection+'/'+owner;
 if(collection==='reportInbox'){if(value?.reporterId===uid)changes[base]=null;return changes;}
 if(collection==='circleSummaries'||collection==='circleReads'){if(value?.[uid])changes[base+'/'+uid]=null;return changes;}
 if(collection==='circles'){if(value?.ownerId===uid){changes[base]=null;changes['circleMessages/'+owner]=null;changes['circleInviteConfig/'+owner]=null;changes['emergencyCircleSignals/'+owner]=null;changes['circleSharing/'+owner]=null;changes['circleLocations/'+owner]=null;changes['circleSummaries/'+owner]=null;changes['circleIndexReady/'+owner]=null;changes['circleReads/'+owner]=null;for(const member of Object.keys(value.members||{}))changes['circleMemberships/'+member+'/'+owner]=null;}else if(value?.members?.[uid]){changes[base+'/members/'+uid]=null;changes[base+'/admins/'+uid]=null;changes['circleSharing/'+owner+'/'+uid]=null;changes['circleLocations/'+owner+'/'+uid]=null;changes['circleSummaries/'+owner+'/'+uid]=null;changes['circleReads/'+owner+'/'+uid]=null;}}
 else if(collection==='circleSharing'||collection==='circleLocations'||collection==='emergencyCircleSignals'){if(value?.[uid])changes[base+'/'+uid]=null;}
 else if(collection==='circleInviteTokens'){if(value?.createdBy===uid)changes[base]=null;}
 else if(collection==='circleMessages'){for(const [id,item]of Object.entries(value||{})){if(item?.senderId===uid)changes[base+'/'+id]=null;else if(item?.reactions?.[uid])changes[base+'/'+id+'/reactions/'+uid]=null;}}
 else if(mirrored.includes(collection)){if(owner===uid)changes[base]=null;else if(value&&Object.prototype.hasOwnProperty.call(value,uid))changes[base+'/'+uid]=null;}
 else if(collection==='handles'){if(value===uid)changes[base]=null;}
 else if(collection==='messageReactions'){if(owner===uid)changes[base]=null;else if(value?.[uid])changes[base+'/'+uid]=null;}
 else if(['notifications','messageInbox','shopComments','reports'].includes(collection)){
  if(owner===uid)changes[base]=null;
  else for(const [id,item]of Object.entries(value||{}))if(item?.[['notifications','messageInbox'].includes(collection)?'senderId':collection==='shopComments'?'authorId':'targetId']===uid)changes[base+'/'+id]=null;
 }
 return changes;
}
async function deleteAccount(db,auth,uid,{deleteMedia}={}){
 const job=db.ref('deletionRequests/'+uid);let state=(await job.get()).val();if(!state||state.status==='complete')return;
 // Disable first, but do not remove Auth until every database cleanup succeeds.
 try{await auth.updateUser(uid,{disabled:true});await auth.revokeRefreshTokens(uid);}catch(e){if(e.code!=='auth/user-not-found')throw e;}
 await job.update({status:'processing'});
 // Keep external-asset ownership records until provider cleanup is confirmed.
 if((await db.ref('mediaAssets/'+uid).get()).exists()){
  if(!deleteMedia)throw Error('External photo cleanup must be configured before deleting this account.');
  await deleteMedia(uid);
 }
 const own=Object.fromEntries(ownCollections.map(c=>[c+'/'+uid,null]));
 await db.ref().update(own);
 const started=Date.now();state=(await job.get()).val();
 for(let stage=state.stage||0;stage<scans.length;stage++){
  let cursor=stage===(state.stage||0)?state.cursor||null:null;
  while(true){
   let query=db.ref(scans[stage]).orderByKey().limitToFirst(1);if(cursor)query=query.startAfter(cursor);
   const snapshot=await query.get(),entries=Object.entries(snapshot.val()||{});if(!entries.length)break;
   const [owner,value]=entries[0];const changes=removalFor(scans[stage],owner,value,uid);cursor=owner;
   changes['deletionRequests/'+uid+'/stage']=stage;changes['deletionRequests/'+uid+'/cursor']=cursor;
   await db.ref().update(changes);
   // Throw while time remains: the retry-enabled trigger resumes from checkpoint.
   if(Date.now()-started>400000)throw Error('Account cleanup checkpoint saved; retry required.');
  }
  await job.update({stage:stage+1,cursor:null});
 }
 try{await auth.deleteUser(uid);}catch(e){if(e.code!=='auth/user-not-found')throw e;}
 await job.set({status:'complete',requestedAt:state.requestedAt,completedAt:Date.now()});
}
module.exports={deleteAccount,removalFor,ownCollections,scans};
