// DOM contract tests for application behavior; not a visual browser test.
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const html=fs.readFileSync('web/index.html','utf8');
function element(){return {value:'',textContent:'',checked:false,disabled:false,hidden:false,children:[],listeners:{},classList:{toggle(){},add(){},remove(){}},setAttribute(){},addEventListener(k,v){this.listeners[k]=v},append(...v){this.children.push(...v)},replaceChildren(){this.children=[]},querySelectorAll(){return []},focus(){}};}
const nodes=Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],element()]));
let mode='success';
let nativeCalls=0;
const sandbox={console,URL,TextEncoder,AbortController,setTimeout,clearTimeout,window:{addEventListener(){}},document:{getElementById(id){assert.ok(nodes[id],`Missing DOM ID ${id}`);return nodes[id]},createElement:element},fetch:async path=>{
 if(path==='/api/live/native/fence'){nativeCalls++;return {ok:true,json:async()=>({proof_id:'server-proof',source_stopped_and_drained:true,cutover_ready:false})};}
 if(path==='/api/live/status')return {ok:false,status:401,json:async()=>({})};
 if(path==='/api/demo/continuous/evidence')return {ok:true,json:async()=>({result:'passed'})};
 if(path==='/api/example/fleet')return {ok:true,json:async()=>({flowContents:{name:'60 synthetic pipelines'}})};
 if(path==='/api/fleet/assess'){if(mode==='offline')throw new Error('network down');return {ok:false,status:422,json:async()=>({report:{ok:false,errors:[{message:'Dependency blocked'}]},inventory:[{id:'mapped',path:['estate','one'],processor_ids:['a','b'],mapped:true},{id:'<script>bad</script>',mapped:false,reason:'Unsupported dependency'},{id:'unknown'}]})};}
 if(path==='/api/example/continuous')return {ok:true,json:async()=>({flowContents:{}})};
 if(mode==='offline')throw new Error('network down');
 const result=mode==='blocked'?{report:{errors:[{message:'Unsupported transform'}],warnings:[]},files:{}}:{report:{errors:[],warnings:[]},files:{'README.md':'Review me'},migration_plan:{}};
 return {ok:mode!=='blocked',status:mode==='blocked'?422:200,json:async()=>result};
}};
vm.createContext(sandbox);vm.runInContext(fs.readFileSync('web/app.js','utf8'),sandbox);
(async()=>{
 await nodes['continuous-example-button'].listeners.click();assert.equal(nodes.target.value,'continuous-worker');
 vm.runInContext('setBusy(true)',sandbox);for(const id of ['target','nifi-version','partial-ack','batch-contract','continuous-example-button'])assert.equal(nodes[id].disabled,true,id);
 vm.runInContext('setBusy(false)',sandbox);
 await vm.runInContext("run('convert')",sandbox);assert.equal(nodes['cutover-guide'].hidden,false);assert.match(nodes.status.textContent,/No migration has started/);assert.equal(nodes['download-button'].disabled,true);
 nodes['review-ack'].checked=true;nodes['review-ack'].listeners.change();assert.equal(nodes['download-button'].disabled,false);
 nodes.document.listeners.input();assert.equal(nodes['download-button'].disabled,true);assert.equal(nodes['cutover-guide'].hidden,true);
 mode='blocked';await vm.runInContext("run('convert')",sandbox);assert.equal(nodes.artifacts.hidden,true);assert.match(nodes.status.textContent,/blockers/);assert.equal(nodes['convert-button'].disabled,false);
 mode='offline';await vm.runInContext("run('convert')",sandbox);assert.match(nodes.status.textContent,/Cannot reach/);assert.ok(nodes.document.value);assert.equal(nodes['convert-button'].disabled,false);
 mode='success';await nodes['fleet-example'].listeners.click();assert.equal(nodes.target.value,'s3-fleet');assert.equal(nodes.source.value,'nifi');assert.match(nodes['fleet-status'].textContent,/Example loaded/);
 await nodes['fleet-assess'].listeners.click();assert.equal(nodes['fleet-results'].hidden,false);assert.equal(nodes['fleet-rows'].children.length,3);assert.match(nodes['fleet-summary'].textContent,/1 mapped · 1 blocked · 1 unknown/);assert.match(nodes['fleet-status'].textContent,/entire cutover remains blocked/);assert.equal(nodes['fleet-rows'].children[1].children[0].textContent,'<script>bad</script>');
 vm.runInContext('setBusy(true)',sandbox);assert.equal(nodes['fleet-assess'].disabled,true);assert.equal(nodes['fleet-example'].disabled,true);vm.runInContext('setBusy(false)',sandbox);
 nodes.document.listeners.input();assert.equal(nodes['fleet-results'].hidden,true);assert.match(nodes['fleet-status'].textContent,/stale/);
 mode='offline';await nodes['fleet-assess'].listeners.click();assert.equal(nodes['fleet-assess'].disabled,false);assert.equal(nodes['fleet-results'].hidden,true);assert.ok(nodes.document.value);
 vm.runInContext("renderFleet({report:{ok:true},inventory:[{id:'one',mapped:true}]})",sandbox);assert.match(nodes['fleet-status'].textContent,/No migration has started/);
 nodes['migration-runtime'].value='airflow';nodes['migration-runtime'].listeners.change();assert.equal(nodes['kafka-migration'].hidden,true);assert.equal(nodes['native-migration'].hidden,false);assert.equal(nodes['native-batch-label'].hidden,false);assert.match(nodes['migration-scope'].textContent,/microbatches/);
 nodes['migration-runtime'].value='camel-k';nodes['migration-runtime'].listeners.change();assert.equal(nodes['native-batch-label'].hidden,true);assert.match(nodes['migration-scope'].textContent,/persistent/);
 nodes['migration-runtime'].value='kafka';nodes['migration-runtime'].listeners.change();assert.equal(nodes['kafka-migration'].hidden,false);assert.equal(nodes['native-migration'].hidden,true);
 vm.runInContext('liveEnabled=true',sandbox);nodes['migration-runtime'].value='airflow';nodes['platform-kind'].value='nifi';nodes['platform-url'].value='https://nifi.example/nifi-api';nodes['platform-group'].value='group';nodes['live-token'].value='test-owner-token';nodes['native-batch'].checked=true;
 await vm.runInContext("nativeOperation('fence')",sandbox);assert.equal(nativeCalls,0);assert.match(nodes['native-status'].textContent,/authorize/);
 nodes['native-fence-ack'].checked=true;nodes['platform-bearer'].value='test-ephemeral-token';await vm.runInContext("nativeOperation('fence')",sandbox);assert.equal(nativeCalls,1);assert.equal(nodes['native-revalidate'].disabled,false);assert.equal(nodes['native-fence-ack'].checked,false);assert.equal(nodes['platform-bearer'].value,'');assert.match(nodes['native-status'].textContent,/NiFi stopped and drained/);
 nodes['platform-group'].listeners.input();assert.equal(nodes['native-revalidate'].disabled,true);assert.equal(nodes['native-result'].hidden,true);
 console.log('UI contract passed: fleet example, mixed coverage, blocked cutover, stale assessment, text-only rendering, busy recovery; native example, busy controls, review gate, stale export invalidation, blockers, offline recovery, handover guide.');
})().catch(e=>{console.error(e);process.exitCode=1});
