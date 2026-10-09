function startCircleSummaries({db,onError=()=>{}}){
 const groups=new Map();let closed=false;
 function listen(ref,receive){ref.on('value',receive,onError);return()=>ref.off('value',receive);}
 function add(snapshot){const id=snapshot.key,g=snapshot.val();if(closed||!g?.members)return;
  let state=groups.get(id);if(!state){state={members:{},messages:[],reads:{},hidden:new Map(),memberStops:new Map(),stops:[],busy:false,dirty:false};groups.set(id,state);
   state.stops.push(listen(db.ref('circleMessages/'+id).limitToLast(100),s=>{state.messages=Object.entries(s.val()||{}).filter(([,m])=>m&&typeof m.text==='string'&&Number.isFinite(m.createdAt)).map(([key,m])=>({...m,id:key,text:m.text.slice(0,160)})).sort((a,b)=>a.createdAt-b.createdAt||a.id.localeCompare(b.id));state.messagesReady=true;void write(id,state);}));
   state.stops.push(listen(db.ref('circleReads/'+id),s=>{state.reads=s.val()||{};state.readsReady=true;void write(id,state);}));
  }
  state.members=g.members;
  for(const [uid,off]of state.memberStops)if(!g.members[uid]){off();state.memberStops.delete(uid);state.hidden.delete(uid);state.signatures?.delete(uid);void db.ref('circleSummaries/'+id+'/'+uid).remove().catch(onError);}
  for(const uid of Object.keys(g.members))if(!state.memberStops.has(uid)){state.hidden.set(uid,null);state.memberStops.set(uid,listen(db.ref('circleHidden/'+uid+'/'+id),s=>{state.hidden.set(uid,s.val()||{});void write(id,state);}));}
  void write(id,state);
 }
 async function write(id,state){
  if(closed||groups.get(id)!==state)return;state.dirty=true;if(state.busy)return;state.busy=true;
  try{while(state.dirty&&!closed&&groups.get(id)===state){state.dirty=false;if(!state.messagesReady||!state.readsReady||[...state.hidden.values()].some(value=>value===null))continue;
   const current=(await db.ref('circles/'+id+'/members').get()).val()||{};
   if(closed||groups.get(id)!==state)return;
   for(const uid of Object.keys(state.members)){if(!current[uid]||(await db.ref('deletionRequests/'+uid).get()).exists())continue;const visible=state.messages.filter(m=>!state.hidden.get(uid)?.[m.id]&&!m.deleted),latest=visible.at(-1)||null,latestIncoming=[...visible].reverse().find(m=>m.senderId!==uid)||null;
    const summary={latest,latestIncoming,unreadCount:visible.filter(m=>m.senderId!==uid&&m.createdAt>(state.reads[uid]||0)).length,updatedAt:latest?.createdAt||0};
    const signature=JSON.stringify(summary);state.signatures??=new Map();if(state.signatures.get(uid)!==signature){await db.ref('circleSummaries/'+id+'/'+uid).set(summary);state.signatures.set(uid,signature);}
   }
   if(!closed&&groups.get(id)===state)await db.ref('circleIndexReady/'+id).set(true);
  }}catch(e){onError(e);}finally{state.busy=false;}
 }
 function remove(snapshot){const id=snapshot.key,state=groups.get(id);if(!state)return;groups.delete(id);state.stops.forEach(off=>off());state.memberStops.forEach(off=>off());void db.ref().update({['circleSummaries/'+id]:null,['circleIndexReady/'+id]:null,['circleReads/'+id]:null}).catch(onError);}
 const root=db.ref('circles');root.on('child_added',add,onError);root.on('child_changed',add,onError);root.on('child_removed',remove,onError);
 return()=>{closed=true;root.off('child_added',add);root.off('child_changed',add);root.off('child_removed',remove);for(const state of groups.values()){state.stops.forEach(off=>off());state.memberStops.forEach(off=>off());}groups.clear();};
}
module.exports={startCircleSummaries};
