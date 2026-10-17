// Admin-only durable references, never copies of message text or raw device tokens.
const {normalizeEvent,eventKey,expiresAt}=require('./notification-policy.cjs');
function createNotificationQueue({db,deliver,now=Date.now,onError=()=>{},retryMs=5000,concurrency=4,maxLocal=200,pollMs=30000}){
 const jobs=new Map(),timers=new Set();let closed=false,running=0,polling=false;
 function later(fn,ms){const timer=setTimeout(()=>{timers.delete(timer);if(!closed)fn();},ms);timer.unref?.();timers.add(timer);}
 function add(event,key){if(closed||jobs.has(key)||jobs.size>=maxLocal)return;const job={event,key,attempt:0,ready:true};jobs.set(key,job);pump();}
 function pump(){if(closed)return;for(const job of jobs.values()){if(running>=concurrency)break;if(!job.ready)continue;job.ready=false;running++;void run(job);}}
 async function run(job){
  const record=db.ref('notificationPending/'+job.key);let retry=false;
  try{
   const event=normalizeEvent(job.event,now());
   if(!event){await record.remove();return;}
   const result=await deliver(event);
   retry=result.status==='busy'||result.status==='no-tokens';
   if(!retry)await record.remove();
  }catch(e){retry=true;onError(e);}
  finally{
   running--;
   if(retry&&!closed&&normalizeEvent(job.event,now())){
    const delay=Math.min(60000,retryMs*2**Math.min(job.attempt++,5));
    try{await record.update({nextAttemptAt:now()+delay});}catch(e){onError(e);}
    later(()=>{job.ready=true;pump();},delay);
   }else jobs.delete(job.key);
   pump();
  }
 }
 async function enqueue(input){
  const event=normalizeEvent(input,now());if(closed||!event)return false;
  const key=eventKey(event);
  if(jobs.has(key))return true;
  await db.ref('notificationPending/'+key).transaction(old=>old&&old.expiresAt>now()?undefined:{event,expiresAt:expiresAt(event),nextAttemptAt:now()});
  add(event,key);return true;
 }
 async function poll(){
  if(closed||polling)return;polling=true;
  try{
   const records=(await db.ref('notificationPending').orderByChild('nextAttemptAt').endAt(now()).limitToFirst(50).get()).val()||{};
   for(const[key,record]of Object.entries(records)){
    if(closed)break;
    if(record?.nextAttemptAt>now())continue;
    const event=normalizeEvent(record?.event,now());
    if(!event||eventKey(event)!==key){await db.ref('notificationPending/'+key).remove();continue;}
    add(event,key);
   }
  }catch(e){onError(e);}finally{polling=false;}
 }
 const interval=setInterval(()=>void poll(),pollMs);interval.unref?.();void poll();
 return {enqueue,poll,pendingCount:()=>jobs.size,stop(){closed=true;clearInterval(interval);timers.forEach(clearTimeout);timers.clear();jobs.clear();}};
}
module.exports={createNotificationQueue};
