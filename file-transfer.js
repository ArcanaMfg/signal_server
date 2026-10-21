/* Bounded sliding window; offsets support retries, duplicates and reordering. */
const FileTransfer = (() => {
  const CHUNK = 480, MAX = 8 * 1024 * 1024;
  let nextId = 0;
  class Receiver {
    constructor({send, progress, complete, fail, digest, headers, chunk, timeout=2500, window=1, fast=false, binary=false, batch=1, acquire=()=>true, release=()=>{}}) {
      Object.assign(this, {send, progress, complete, fail, digest, timeout, headers, chunk, acquire, release, fast, binary});
      this.window=Math.max(1,Math.min(4,Math.trunc(window)||1));
      this.batch=Math.max(1,Math.min(8,Math.trunc(batch)||1));
    }
    start(path='index.html') {
      this.cancel();
      this.state = {id:++nextId,path,offset:0,next:0,parts:[],pending:new Map(),groups:new Set(),buffered:new Map(),
        chunk:CHUNK,batch:1,started:performance.now(),requests:0,retries:0,maxPending:0};
      this.fill();
    }
    cancel() {
      const previous=this.state;
      if(this.state) for(const pending of this.state.pending.values()) clearTimeout(pending.timer);
      if(this.state) {
        // Idempotent repeated release tolerates a dropped SCTP packet.
        try { for(let n=0;n<2;n++) this.send(JSON.stringify({type:'file-close',id:this.state.id})); } catch {}
      }
      this.state = null;
      if(previous) for(const group of previous.groups) this.release();
    }
    error(message) { this.cancel(); this.fail(new Error(message)); }
    fill() {
      const s=this.state;
      if(!s || s.verifying) return;
      if(s.total===undefined) { if(!s.pending.has(0)) this.request(0); return; }
      while(this.state===s && s.next<s.total && s.next<s.offset+this.window*s.batch*s.chunk && s.groups.size<this.window) {
        const count=Math.min(s.batch,Math.ceil((s.total-s.next)/s.chunk),
          Math.floor((s.offset+this.window*s.batch*s.chunk-s.next)/s.chunk));
        const offset=s.next;if(!this.request(offset,count)) break;s.next+=s.chunk*count;
      }
    }
    finishPending(s,offset) {
      const pending=s.pending.get(offset);
      clearTimeout(pending.timer);s.pending.delete(offset);
      if(--pending.group.remaining===0) {s.groups.delete(pending.group);this.release();}
    }
    request(offset,count=1) {
      const s=this.state;if(!s || s.verifying) return;
      let pending=s.pending.get(offset);
      if(!pending) {
        if(!this.acquire()) return false;
        const group={remaining:count};s.groups.add(group);
        for(let n=0;n<count;n++)s.pending.set(offset+n*s.chunk,{attempts:0,group});
        pending=s.pending.get(offset);s.maxPending=Math.max(s.maxPending,s.pending.size);
      }
      const wait=this.fast && s.total===undefined?Math.max(this.timeout,10000):this.timeout;
      for(let n=0;n<count;n++) {
        const pos=offset+n*s.chunk,p=s.pending.get(pos);
        if(++p.attempts>5) {this.error('ファイル転送がタイムアウトしました。再取得してください。');return;}
        if(p.attempts>1)s.retries++;
        clearTimeout(p.timer);
        // A lost frame is retried on its own, without resending the whole batch.
        p.timer=setTimeout(()=>{if(this.state===s)this.request(pos);},wait);
      }
      s.requests++;
      try {this.send(JSON.stringify({type:'file-get',id:s.id,path:s.path,offset,...(this.fast && s.total===undefined?{v:2,info:true,...(this.binary?{binary:true}:{})}:s.protocol>=2?{v:s.protocol}:{}),...(count>1?{count}:{})}));}
      catch(e){this.error(e.message);}
      return true;
    }
    async receive(data) {
      let p,frame;
      if(typeof data==='string') {
        try { p=JSON.parse(data); } catch { return false; }
      } else {
        if(ArrayBuffer.isView(data)) frame=new Uint8Array(data.buffer,data.byteOffset,data.byteLength);
        else if(data instanceof ArrayBuffer) frame=new Uint8Array(data);
        else return false;
        if(frame.length<8 || frame[0]!==67 || frame[1]!==54 || frame[2]!==70 || frame[3]!==51) return false;
        const view=new DataView(frame.buffer,frame.byteOffset,frame.byteLength);
        p={type:'file-chunk',id:view.getUint32(4)};
        if(frame.length>=16) Object.assign(p,{offset:view.getUint32(8),length:view.getUint16(12),reserved:view.getUint16(14)});
      }
      if (!['file-chunk','file-info','file-error'].includes(p?.type)) return false;
      const s = this.state;
      if (!s) return false;
      if(p.id !== s.id) return false;
      if(s.verifying) return true;
      if (p.type === 'file-error') {
        const error = new Error(p.message || 'ファイルを取得できません'); error.status=p.code;
        this.cancel(); this.fail(error); return true;
      }
      if(p.type==='file-info') {
        try {
          if(!this.fast || s.total!==undefined || !s.pending.has(0)) return true;
          const format=p.v===2 && p.chunk===768 || this.binary && p.v===3 && p.chunk===1136 && p.encoding==='binary';
          if(!format || p.path!==s.path || !Number.isInteger(p.total) || p.total<0 || p.total>MAX || !/^[a-f0-9]{64}$/.test(p.sha256)) throw new Error('ファイル情報が不正です');
          this.finishPending(s,0);
          s.total=p.total;s.sha256=p.sha256;s.chunk=p.chunk;s.protocol=p.v;s.next=0;
          s.batch=p.v===3 && Number.isInteger(p.batch) && p.batch>=1 && p.batch<=8?Math.min(this.batch,p.batch):1;
          s.firstByteMs=performance.now()-s.started;
          this.headers?.({total:p.total,mime:p.mime || 'application/octet-stream',sha256:p.sha256});
          if(!s.total) this.request(0); else this.fill();
        } catch(e) {if(this.state===s)this.error(e.message);}
        return true;
      }
      if(frame && (s.protocol!==3 || frame.length<16 || p.reserved!==0 || p.length>1136 || frame.length!==16+p.length)) {
        this.error('バイナリ転送情報が不正です');return true;
      }
      if(s.protocol>=2) p={...p,path:s.path,total:s.total,sha256:s.sha256};
      if (!s.pending.has(p.offset)) return true; // Late or duplicate response.
      try {
        if (p.path !== s.path || !Number.isInteger(p.total) || p.total < 0 || p.total > MAX ||
            !/^[a-f0-9]{64}$/.test(p.sha256) ||
            (s.protocol===3?!frame:typeof p.data!=='string' || p.data.length>s.chunk*4/3) ||
            (s.total !== undefined && (s.total !== p.total || s.sha256 !== p.sha256))) throw new Error('ファイル転送情報が不正です');
        const bytes = frame?frame.slice(16):Uint8Array.from(atob(p.data), c => c.charCodeAt(0));
        if (bytes.length !== Math.min(s.chunk, p.total-p.offset)) throw new Error('ファイルの受信サイズが一致しません');
        this.finishPending(s,p.offset);
        if(s.total===undefined) {
          s.firstByteMs=performance.now()-s.started;
          this.headers?.({total:p.total,mime:p.mime || 'application/octet-stream',sha256:p.sha256});
          s.total=p.total;s.sha256=p.sha256;s.next=s.chunk;
        }
        s.buffered.set(p.offset,bytes);
        while(s.buffered.has(s.offset)) {
          const part=s.buffered.get(s.offset);s.buffered.delete(s.offset);
          s.parts.push(part);s.offset+=part.length;this.chunk?.(part);
          this.progress(s.offset,s.total);
          if(!part.length) break;
        }
        if(s.offset<s.total) {this.fill();return true;}
        s.verifying = true;
        const result = new Uint8Array(s.total);
        let pos=0;
        for (const part of s.parts) { result.set(part, pos); pos += part.length; }
        const hash = await this.digest(result);
        if (this.state !== s) return true;
        if (hash !== s.sha256) throw new Error('SHA-256が一致しません。転送を中止しました');
        const html = s.path === 'index.html' ? new TextDecoder('utf-8', {fatal:true}).decode(result) : undefined;
        const metrics={protocol:s.protocol || 1,chunk:s.chunk,batch:s.batch,window:this.window,maxPending:s.maxPending,bytes:s.total,elapsedMs:performance.now()-s.started,firstByteMs:s.firstByteMs,requests:s.requests,retries:s.retries};
        this.cancel(); this.complete({html, bytes:result, sha256:hash,metrics});
      } catch(e) { if (this.state === s) this.error(e.message); }
      return true;
    }
  }
  return {Receiver};
})();
if (typeof module !== 'undefined') module.exports = FileTransfer;
