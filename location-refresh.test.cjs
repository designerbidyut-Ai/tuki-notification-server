const {test}=require('node:test'),assert=require('node:assert/strict');
const {createLocationRefresh}=require('./location-refresh.cjs');
function setup(extra={}){
 const values={'visibility/alice':'public','visibility/bob':'public','trackingDiagnostics/bob/native':{schema:1,source:'native',runtime:'android-build-16'},'pushTokens/bob':{phone:'test-token'},...extra},sent=[];let time=1700000000000,afterLease=null;
 const db={ref:path=>({get:async()=>({val:()=>values[path]??null}),transaction:async fn=>{const next=fn(values[path]??null);if(next===undefined)return{committed:false};values[path]=next;afterLease?.();return{committed:true};}})};
 const handler=createLocationRefresh({auth:{verifyIdToken:async token=>{if(token!=='test-auth')throw Error('invalid');return{uid:'alice'};}},db,messaging:{sendEachForMulticast:async data=>{sent.push(data);return{responses:[{success:true}]};}},now:()=>time});
 return {values,sent,advance:ms=>time+=ms,afterLease:fn=>afterLease=fn,call:async(body={recipient:'bob'},token='test-auth')=>{let status=200,result;await handler({body,get:()=>token?'Bearer '+token:''},{set(){},status(code){status=code;return this;},json(body){result=body;}});return {status,result};}};
}
test('refresh authenticates viewer and sends a bounded data-only wake, never a claimed GPS result',async()=>{
 const s=setup();assert.equal((await s.call({recipient:'bob'},'')).status,401);assert.equal((await s.call({recipient:'bob'},'wrong')).status,401);
 assert.equal((await s.call({recipient:'../bob'})).status,400);assert.equal((await s.call()).status,202);
 const message=s.sent[0];assert.equal(message.notification,undefined);assert.equal(message.android.priority,'high');assert.equal(message.android.ttl,45000);assert.equal(message.data.recipientId,'bob');assert.equal(message.data.requesterId,'alice');assert(!JSON.stringify(message).includes('latitude'));
});
test('privacy and reciprocal audience restrictions match map access',async()=>{
 for(const extra of [{'visibility/alice':'only_me'},{'visibility/bob':'only_me'},{'visibility/bob':'friends'},{'visibility/alice':'friends'},{'blocks/bob/alice':true},{'blocks/alice/bob':true},{'deletionRequests/bob':{}},{'deletionRequests/alice':{}}]){const s=setup(extra);assert.equal((await s.call()).status,403);assert.equal(s.sent.length,0);}
 const s=setup({'visibility/alice':'friends','visibility/bob':'friends','friends/alice/bob':true,'friends/bob/alice':true});assert.equal((await s.call()).status,202);
});
test('circle refresh needs both members and explicit target sharing',async()=>{
 const base={'visibility/alice':'only_me','visibility/bob':'only_me','circles/family/members/alice':true,'circles/family/members/bob':true,'circleSharing/family/bob':true};
 assert.equal((await setup(base).call({recipient:'bob',circleId:'family'})).status,202);
 for(const path of ['circles/family/members/alice','circles/family/members/bob','circleSharing/family/bob'])assert.equal((await setup({...base,[path]:null}).call({recipient:'bob',circleId:'family'})).status,403);
});
test('repeated refresh is throttled and privacy is rechecked immediately before sending',async()=>{
 const s=setup();assert.equal((await s.call()).status,202);assert.equal((await s.call()).status,429);s.advance(11000);assert.equal((await s.call()).status,429);assert.equal(s.sent.length,1);s.advance(31000);assert.equal((await s.call()).status,202);
 const revoked=setup();revoked.afterLease(()=>revoked.values['blocks/bob/alice']=true);assert.equal((await revoked.call()).status,403);assert.equal(revoked.sent.length,0);
});
test('missing device tokens are reported unavailable without claiming freshness',async()=>{const s=setup({'pushTokens/bob':null});assert.equal((await s.call()).status,409);assert.equal(s.sent.length,0);});
test('old phones without a native receiver are never sent silent high-priority refreshes',async()=>{for(const value of [null,{schema:1,source:'app',runtime:'android-build-16'}]){const s=setup({'trackingDiagnostics/bob/native':value});assert.equal((await s.call()).status,409);assert.equal(s.sent.length,0);}});
