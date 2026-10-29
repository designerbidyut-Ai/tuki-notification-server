const {createHash}=require('node:crypto');
const windows={message:3600000,circle:3600000,friend:86400000,emergency:300000,city_join:86400000};
const validKey=value=>typeof value==='string'&&value.length>0&&value.length<=128&&!/[.#$\[\]/\u0000-\u0020]/.test(value);
function normalizeEvent(event,now=Date.now()){
 if(!event||!windows[event.type]||!['recipient','sender','id'].every(k=>validKey(event[k]))||event.sender===event.recipient)return null;
 if(event.type==='circle'&&!validKey(event.circleId))return null;
 const at=event.type==='emergency'?event.startedAt:event.createdAt;
 if(!Number.isFinite(at)||at<=0||at>now+60000||at+windows[event.type]<=now)return null;
 return {type:event.type,recipient:event.recipient,sender:event.sender,id:event.id,
  ...(event.type==='circle'?{circleId:event.circleId}:{}),...(event.type==='emergency'?{startedAt:at}:{createdAt:at})};
}
function eventKey(e){return createHash('sha256').update([e.type,e.recipient,e.sender,...(e.type==='circle'?[e.circleId]:[]),e.id].join(':')).digest('hex');}
function expiresAt(e){return (e.type==='emergency'?e.startedAt:e.createdAt)+windows[e.type];}
module.exports={normalizeEvent,eventKey,expiresAt,windows,validKey};
