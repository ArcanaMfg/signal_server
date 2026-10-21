'use strict';
class SdViewer {
  constructor({send,digest,progress,log=()=>{},fail,batch=2}) {
    Object.assign(this,{send,digest,progress,log,fail}); this.queue=[]; this.running=new Set();
    this.batch=batch;
    this.inflight=0;
    this.loading=null;
    this.token=Array.from(crypto.getRandomValues(new Uint8Array(16)),n=>n.toString(16).padStart(2,'0')).join('');
    navigator.serviceWorker?.addEventListener('message',event=>{
      const p=event.data;
      if(p?.type!=='sd-fetch' || p.token!==this.token || !event.ports[0]) return;
      const port=event.ports[0];
      if(event.source?.scriptURL!==this.worker?.scriptURL) { port.close();return; }
      const job={port,path:p.path};
      port.onmessage=e=>{if(e.data?.type==='cancel'){job.cancelled=true;job.receiver?.cancel();this.finish(job);}};
      if(this.loading) job.loading=this.loading;
      this.queue.push(job);this.pump();
    });
  }
  beginLoading() {
    this.loading={root:null,received:0,total:0,percent:0};
    this.progress(0);
  }
  updateProgress(job,received,total) {
    const load=this.loading;
    if(!load || job.loading!==load) return;
    if(job.path==='index.html' && !load.root) load.root=job;
    if(job!==load.root) {
      load.received+=received-(job.received||0);
      load.total+=total-(job.total||0);
    }
    job.received=received;job.total=total;
    // Discovery is incremental. Reserve 15% for the document and the rest for
    // discovered resources; onload alone completes the initial page load.
    const root=load.root, documentRatio=root?.total?root.received/root.total:root?.complete?1:0;
    const resourceRatio=load.total?load.received/load.total:0;
    const percent=Math.min(99,Math.floor(15*documentRatio+84*resourceRatio));
    load.percent=Math.max(load.percent,percent);
    this.progress(load.percent);
  }
  completeLoading() {
    if(!this.loading) return;
    this.loading=null;this.progress(100);
  }
  async open() {
    this.beginLoading();
    if(!navigator.serviceWorker) throw new Error('HTTPSとService Worker対応ブラウザが必要です');
    const registration=await navigator.serviceWorker.register('p2p-sw.js?v=034',{scope:'./p2p/'});
    const worker=registration.installing || registration.waiting || registration.active;
    if(worker.state!=='activated') await new Promise((resolve,reject)=>{
      const timeout=setTimeout(()=>reject(new Error('Service Worker起動タイムアウト')),15000);
      worker.addEventListener('statechange',()=>{
        if(worker.state==='activated'){clearTimeout(timeout);resolve();}
        if(worker.state==='redundant'){clearTimeout(timeout);reject(new Error('Service Workerの更新に失敗しました'));}
      });
    });
    this.worker=registration.active;
    const id=await new Promise((resolve,reject)=>{
      const channel=new MessageChannel(),timeout=setTimeout(()=>reject(new Error('接続ページを識別できません')),5000);
      channel.port1.onmessage=e=>{clearTimeout(timeout);channel.port1.close();resolve(e.data.client);};
      this.worker.postMessage({type:'identify'},[channel.port2]);
    });
    return new URL(`./p2p/${id}/${this.token}/index.html`,location.href).href;
  }
  finish(job) {
    this.running.delete(job);this.queue=this.queue.filter(j=>j!==job);job.port.close();this.pump();
  }
  refill() {
    const limit=Math.max(1,Math.floor(2/Math.max(1,this.running.size)));
    for(const job of this.running) if(job.receiver) { job.receiver.window=limit;job.receiver.fill(); }
  }
  pump() {
    while(this.running.size<3 && this.queue.length) {
      const job=this.queue.shift(); if(job.cancelled) continue;
      this.running.add(job);
      const post=p=>job.port.postMessage(p);
      job.receiver=new FileTransfer.Receiver({send:this.send,digest:this.digest,fast:true,binary:true,batch:this.batch,
        window:Math.max(1,Math.floor(2/this.running.size)),
        acquire:()=>{if(this.inflight>=2)return false;this.inflight++;return true;},
        release:()=>{this.inflight--;queueMicrotask(()=>this.refill());},
        headers:p=>{this.updateProgress(job,0,p.total);post({type:'headers',...p});},chunk:bytes=>post({type:'chunk',bytes}),
        progress:(bytes,total)=>{
          const now=performance.now();
          if(!job.lastProgress || now-job.lastProgress>=100 || bytes===total) { job.lastProgress=now;this.updateProgress(job,bytes,total); }
        },
        complete:({bytes,sha256,metrics})=>{
          job.complete=true;this.updateProgress(job,bytes.length,bytes.length);
          this.log(`SD受信完了: ${job.path} / ${bytes.length} bytes / ${(metrics.elapsedMs/1000).toFixed(2)}秒 / 再送${metrics.retries} / SHA-256=${sha256}`);
          post({type:'done'});this.finish(job);
        },
        fail:e=>{post({type:'error',message:e.message,status:e.status});this.log(`${job.path}: ${e.message}`);this.fail?.(job.path,e);this.finish(job);}
      });
      job.receiver.start(job.path);
    }
    this.refill();
  }
  async receive(text) {
    for(const job of this.running) if(await job.receiver.receive(text)) return true;
    try { return /^file-/.test(JSON.parse(text)?.type); } catch { return false; }
  }
  get state(){return this.running.size || this.queue.length;}
  cancel() {
    this.loading=null;
    const jobs=[...this.queue,...this.running]; this.queue=[];this.running.clear();
    for(const job of jobs){job.receiver?.cancel();job.port.postMessage({type:'error',message:'P2P接続が終了しました',status:503});job.port.close();}
  }
}
