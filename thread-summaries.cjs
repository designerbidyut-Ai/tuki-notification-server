const {createHash}=require('node:crypto');
// Reuse the notification worker's existing message snapshots. Clients read small
// previews instead of downloading 100 messages for every closed conversation.
function createThreadSummaries({db,now=Date.now,onError=()=>{}}){
 const owners=new Map(),pending=new Map(),pruned=new Map();let closed=false;
 const key=(owner,peer)=>owner+'/'+peer;
 function write(owner,peer){
  const id=key(owner,peer);if(pending.has(id))return;
  const work=Promise.resolve().then(async()=>{
   const state=owners.get(owner),thread=state?.threads.get(peer);if(closed||!state?.readsReady||!thread?.ready)return;
   if((await db.ref('deletionRequests/'+owner).get()).exists()||closed||owners.get(owner)!==state||state.threads.get(peer)!==thread)return;
   const messages=thread.messages,readAt=state.reads[peer]||0;
   const summary=messages.length?{latest:messages.at(-1),latestIncoming:[...messages].reverse().find(m=>m.senderId!==owner)||null,unreadCount:messages.filter(m=>m.senderId!==owner&&m.createdAt>readAt).length,updatedAt:Math.max(messages.at(-1).createdAt,state.index[peer]||0)}:null;
   const signature=JSON.stringify(summary);if(thread.signature!==signature){await db.ref('threadSummaries/'+id).set(summary);thread.signature=signature;}
   if(!closed&&owners.get(owner)===state&&[...state.threads.values()].every(t=>t.ready)&&state.readsReady&&!state.ready){await db.ref('threadIndexReady/'+owner).set(true);state.ready=true;}
  }).catch(onError).finally(()=>{pending.delete(id);const state=owners.get(owner),thread=state?.threads.get(peer);if(thread?.dirty){thread.dirty=false;write(owner,peer);}});
  pending.set(id,work);
 }
 function watchOwner(owner){
  if(owners.has(owner))return;
  const state={threads:new Map(),reads:{},readsReady:false,index:{},ready:false,stop:()=>{}};owners.set(owner,state);
  const source=db.ref('chatReads/'+owner),receive=s=>{if(closed||owners.get(owner)!==state)return;state.reads=s.val()||{};state.readsReady=true;for(const[peer,thread]of state.threads){thread.dirty=true;write(owner,peer);}};
  source.on('value',receive,onError);state.stop=()=>source.off('value',receive);
 }
 return {
  threads(owner,index){if(closed)return;watchOwner(owner);const state=owners.get(owner);state.index=index;
   for(const peer of state.threads.keys())if(!Object.hasOwn(index,peer)){state.threads.delete(peer);void db.ref('threadSummaries/'+key(owner,peer)).remove().catch(onError);}
   for(const peer of Object.keys(index))if(!state.threads.has(peer))state.threads.set(peer,{ready:false,messages:[],dirty:false});
  },
  messages(owner,peer,values){
   if(closed)return;watchOwner(owner);const state=owners.get(owner);if(!Object.hasOwn(state.index,peer))return;let thread=state.threads.get(peer);if(!thread){thread={ready:false,messages:[],dirty:false};state.threads.set(peer,thread);}
   thread.messages=Object.entries(values||{}).filter(([,m])=>m&&!m.deleted&&typeof m.text==='string'&&Number.isFinite(m.createdAt)).map(([id,m])=>({id,senderId:m.senderId,text:m.text.slice(0,160),createdAt:m.createdAt})).sort((a,b)=>a.createdAt-b.createdAt||a.id.localeCompare(b.id)).slice(-100);
   thread.ready=true;thread.dirty=true;write(owner,peer);
  },
  async incoming(event){
   if(closed||event.type!=='message'||!Number.isFinite(event.createdAt)||event.createdAt<now()-300000||event.createdAt>now()+60000)return;
   const {recipient,sender}=event;
   const eligibility=await Promise.all([`blocks/${recipient}/${sender}`,`blocks/${sender}/${recipient}`,`deletionRequests/${recipient}`,`deletionRequests/${sender}`].map(path=>db.ref(path).get()));
   if(closed||eligibility.some(s=>s.exists()))return;
   const [read,message]=await Promise.all([db.ref(`chatReads/${recipient}/${sender}`).get(),db.ref(`messages/${recipient}/${sender}/${event.id}`).get()]);
   const value=message.val();if(closed||read.val()>=event.createdAt||!value||value.deleted||value.senderId!==sender||value.createdAt!==event.createdAt||typeof value.text!=='string')return;
   const id=createHash('sha256').update(event.sender+':'+event.id).digest('hex');
   await db.ref('messageInbox/'+recipient+'/'+id).set({id:event.id,senderId:sender,text:value.text.slice(0,160),createdAt:event.createdAt});
   // This feed is for recent popups. Full conversation history stays canonical.
   if(!pruned.has(recipient)||now()-pruned.get(recipient)>3600000){
    pruned.delete(recipient);pruned.set(recipient,now());while(pruned.size>256)pruned.delete(pruned.keys().next().value);
    const expired=await db.ref('messageInbox/'+recipient).orderByChild('createdAt').endAt(now()-86400000).limitToFirst(100).get();
    const removals=Object.fromEntries(Object.keys(expired.val()||{}).map(key=>[key,null]));
    if(!closed&&Object.keys(removals).length)await db.ref('messageInbox/'+recipient).update(removals);
   }
  },
  removeOwner(owner){owners.get(owner)?.stop();owners.delete(owner);},
  stop(){closed=true;owners.forEach(state=>state.stop());owners.clear();},
 };
}
module.exports={createThreadSummaries};
