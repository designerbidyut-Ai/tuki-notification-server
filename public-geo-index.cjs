// Maintain the index for older installed clients too. Account visibility remains
// canonical; this never creates a public record for a private person.
function cell(point){
 const y=Math.min(1799,Math.floor((point.latitude+90)*10)),x=Math.min(3599,Math.floor((point.longitude+180)*10));
 return String(y).padStart(4,'0')+'-'+String(x).padStart(4,'0');
}
function startPublicGeoIndex({db,onError=()=>{}}){
 const source=db.ref('publicLocations'),queues=new Map();let closed=false;
 const changed=snapshot=>{
  const uid=snapshot.key;if(!uid||closed)return;
  const previous=queues.get(uid)||Promise.resolve();
  const work=previous.then(async()=>{
   if(closed)return;
   // Transaction prevents a delayed old GPS fix from overwriting a newer one,
   // or recreating the public record after a privacy change.
   await source.child(uid).transaction(value=>{
    if(closed||!value||!Number.isFinite(value.latitude)||!Number.isFinite(value.longitude)||Math.abs(value.latitude)>90||Math.abs(value.longitude)>180||!Number.isInteger(value.timestamp)||value.timestamp<1000000000000||value.timestamp>9999999999999)return;
    const geoIndex=cell(value)+':'+value.timestamp;
    return value.geoIndex===geoIndex?undefined:{...value,geoIndex};
   });
  }).catch(onError).finally(()=>{if(queues.get(uid)===work)queues.delete(uid);});queues.set(uid,work);
 };
 for(const event of ['child_added','child_changed'])source.on(event,changed,onError);
 return()=>{closed=true;for(const event of ['child_added','child_changed'])source.off(event,changed);};
}
module.exports={cell,startPublicGeoIndex};
