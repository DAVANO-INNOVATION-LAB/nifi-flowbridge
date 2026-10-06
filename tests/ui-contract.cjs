// DOM contract tests for application behavior; not a visual browser test.
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const html=fs.readFileSync('web/index.html','utf8');
function element(){return {value:'',textContent:'',checked:false,disabled:false,hidden:false,children:[],listeners:{},classList:{toggle(){},add(){},remove(){}},setAttribute(){},addEventListener(k,v){this.listeners[k]=v},append(...v){this.children.push(...v)},replaceChildren(){this.children=[]},querySelectorAll(){return []},focus(){}};}
const nodes=Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],element()]));
let mode='success';
const sandbox={console,TextEncoder,AbortController,setTimeout,clearTimeout,window:{addEventListener(){}},document:{getElementById(id){assert.ok(nodes[id],`Missing DOM ID ${id}`);return nodes[id]},createElement:element},fetch:async path=>{
 if(path==='/api/live/status')return {ok:false,status:401,json:async()=>({})};
 if(path==='/api/demo/continuous/evidence')return {ok:true,json:async()=>({result:'passed'})};
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
 console.log('UI contract passed: native example, busy controls, review gate, stale export invalidation, blockers, offline recovery, handover guide.');
})().catch(e=>{console.error(e);process.exitCode=1});
