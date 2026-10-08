const fs=require('node:fs'),path=require('node:path'),admin=require('firebase-admin'),express=require('express');
const {createNotificationWorker}=require('./notification-core.cjs');
const {startFollowersSync}=require('./followers-sync.cjs');
const {createMessageWake}=require('./message-wake.cjs');
function credential(){
 if(process.env.FIREBASE_SERVICE_ACCOUNT){
  const raw=process.env.FIREBASE_SERVICE_ACCOUNT.trim();
  try{return JSON.parse(raw.startsWith('{')?raw:Buffer.from(raw,'base64').toString('utf8'));}catch{throw Error('Invalid FIREBASE_SERVICE_ACCOUNT JSON.');}
 }
 for(const file of [path.join(__dirname,'../.secrets/firebase-service-account.json'),path.join(__dirname,'.secrets/firebase-service-account.json')])if(fs.existsSync(file))return JSON.parse(fs.readFileSync(file,'utf8'));
 throw Error('Firebase service account is missing.');
}
const account=credential();admin.initializeApp({credential:admin.credential.cert(account),databaseURL:process.env.FIREBASE_DATABASE_URL||'https://livelocation-afb04-default-rtdb.asia-southeast1.firebasedatabase.app'});
const database=admin.database();
const fail=e=>console.error('Notification worker error:',e.code||e.name||'unknown');
const threadIndex=require('./thread-summaries.cjs').createThreadSummaries({db:database,onError:fail});
const worker=createNotificationWorker({db:database,messaging:admin.messaging(),onError:fail,threadIndex});
const stopGeo=require('./public-geo-index.cjs').startPublicGeoIndex({db:database,onError:fail});
const {createCloudinaryMedia,createProfileMedia,startMediaSweep}=require('./profile-media.cjs');
const mediaStorage=createCloudinaryMedia({cloudName:process.env.CLOUDINARY_CLOUD_NAME,apiKey:process.env.CLOUDINARY_API_KEY,apiSecret:process.env.CLOUDINARY_API_SECRET});
const stopMediaSweep=startMediaSweep({db:database,storage:mediaStorage,onError:fail});
worker.stats.followersSynced=0;
const stopFollowers=startFollowersSync({db:database,onSynced:()=>worker.stats.followersSynced++,onError:e=>{worker.stats.errors++;fail(e);}});
let connected=false;
const connection=database.ref('.info/connected'),onConnection=snapshot=>{connected=snapshot.val()===true;};
connection.on('value',onConnection,fail);
const app=express(),startedAt=new Date().toISOString();
app.get('/',(_req,res)=>res.json({service:'tuki-notification-worker',version:'notification-world-ready-2026-10-08',status:connected?'online':'disconnected',connected,startedAt,features:{publicGeoIndex:true,threadSummaries:true,profileMedia:!!mediaStorage},stats:worker.stats,pending:worker.pendingCount()}));
app.get('/health',(_req,res)=>res.status(connected?200:503).json({status:connected?'online':'disconnected',connected,pending:worker.pendingCount(),errors:worker.stats.errors}));
app.post('/profile-media',express.json({limit:'320kb'}),createProfileMedia({auth:admin.auth(),db:database,storage:mediaStorage}));
app.post('/message-wake',express.json({limit:'2kb'}),createMessageWake({auth:admin.auth(),db:database,worker}));
const server=app.listen(Number(process.env.TUKI_WORKER_PORT||process.env.PORT||3000),process.env.TUKI_WORKER_HOST||'0.0.0.0',()=>console.log('Tuki notification worker running; chat, friend requests and SOS enabled.'));
function stop(){worker.stop();threadIndex.stop();stopGeo();stopFollowers();stopMediaSweep();connection.off('value',onConnection);server.close();void admin.app().delete().finally(()=>process.exit(0));}
process.on('SIGINT',stop);process.on('SIGTERM',stop);
