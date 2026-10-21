/* Virtual static HTTP filesystem. Content is read from the device, never cached. */
'use strict';
self.addEventListener('install', event => event.waitUntil(self.skipWaiting()));
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
self.addEventListener('message', event => {
  if(event.data?.type === 'identify') event.ports[0]?.postMessage({client:event.source.id});
});
const prefix = new URL('./p2p/',self.location.href).pathname;
function route(url) {
  if(url.origin !== self.location.origin || !url.pathname.startsWith(prefix)) return null;
  const parts=url.pathname.slice(prefix.length).split('/');
  if(parts.length<3 || !parts[0] || !/^[a-f0-9]{32}$/.test(parts[1])) return null;
  return {owner:parts[0],token:parts[1],encoded:parts.slice(2).join('/')};
}
function failure(message,status=502) {
  return new Response(message,{status,headers:{'Content-Type':'text/plain; charset=utf-8','Cache-Control':'no-store'}});
}
async function serve(event, selected) {
  const req=event.request;
  if(req.method!=='GET' && req.method!=='HEAD') return failure('Static files support GET and HEAD only',405);
  let path;
  try { path=decodeURIComponent(selected.encoded || 'index.html'); } catch { return failure('Invalid URL encoding',400); }
  if(path.endsWith('/')) path+='index.html';
  if(!path || new TextEncoder().encode(path).length>160 || path.split('/').some(p=>!p || p==='.' || p==='..') || /[\\\x00-\x1f\x7f:*?"<>|]/.test(path)) return failure('Invalid SD path',400);
  const owner=await self.clients.get(selected.owner);
  if(!owner) return failure('接続元のページを開いてESP32へ再接続してください',503);
  const channel=new MessageChannel();
  let streamController, settled=false, headerReceived=false, rangeStart=0,rangeEnd=Infinity,position=0;
  let resolveResponse;
  const result=new Promise(resolve=>{resolveResponse=resolve;});
  let timer;
  const cleanup=()=>{clearTimeout(timer);channel.port1.close();};
  const abort=(message,status=502)=>{
    if(settled) return; settled=true;
    if(headerReceived) streamController?.error(new Error(message));
    else resolveResponse(failure(message,status));
    try { channel.port1.postMessage({type:'cancel'}); } catch {}
    cleanup();
  };
  const touch=()=>{clearTimeout(timer);timer=setTimeout(()=>abort('P2P転送がタイムアウトしました',504),20000);};
  channel.port1.onmessage=event=>{
    if(settled) return;
    const p=event.data; touch();
    if(p.type==='headers') {
      if(headerReceived || !Number.isInteger(p.total) || p.total<0 || p.total>8*1024*1024) { abort('Invalid file metadata'); return; }
      let status=200;
      const headers={'Content-Type':p.mime,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Accept-Ranges':'bytes'};
      const range=req.headers.get('Range');
      if(range) {
        const m=/^bytes=(\d*)-(\d*)$/.exec(range);
        if(!m || (!m[1] && !m[2]) || !p.total) {
          settled=true; resolveResponse(new Response(null,{status:416,headers:{'Content-Range':`bytes */${p.total}`}}));
          channel.port1.postMessage({type:'cancel'});cleanup();return;
        }
        rangeStart=m[1]?Number(m[1]):Math.max(0,p.total-Number(m[2]));
        rangeEnd=m[1]?(m[2]?Math.min(Number(m[2]),p.total-1):p.total-1):p.total-1;
        if(!Number.isSafeInteger(rangeStart) || !Number.isSafeInteger(rangeEnd) || rangeStart>rangeEnd || rangeStart>=p.total) {
          settled=true;resolveResponse(new Response(null,{status:416,headers:{'Content-Range':`bytes */${p.total}`}}));
          channel.port1.postMessage({type:'cancel'});cleanup();return;
        }
        status=206; headers['Content-Range']=`bytes ${rangeStart}-${rangeEnd}/${p.total}`;
      } else rangeEnd=p.total-1;
      headers['Content-Length']=String(Math.max(0,rangeEnd-rangeStart+1));
      if(req.method==='HEAD') {
        settled=true; resolveResponse(new Response(null,{status,headers}));
        channel.port1.postMessage({type:'cancel'});cleanup();return;
      }
      const body=new ReadableStream({start(controller){streamController=controller;},cancel(){
        settled=true;channel.port1.postMessage({type:'cancel'});cleanup();
      }});
      headerReceived=true;resolveResponse(new Response(body,{status,headers}));
    } else if(p.type==='chunk') {
      if(!headerReceived || !(p.bytes instanceof Uint8Array)) { abort('Invalid transfer sequence');return; }
      const a=Math.max(0,rangeStart-position),b=Math.min(p.bytes.length,rangeEnd-position+1);
      if(b>a) streamController.enqueue(p.bytes.slice(a,b));
      position+=p.bytes.length;
    } else if(p.type==='done') {
      if(!headerReceived) { abort('Missing file headers');return; }
      settled=true;streamController.close();cleanup();
    } else if(p.type==='error') abort(p.message,p.status || 502);
  };
  touch();
  owner.postMessage({type:'sd-fetch',token:selected.token,path},[channel.port2]);
  // Keep the worker alive until completion as well as until response headers.
  event.waitUntil(new Promise(resolve=>{
    const check=setInterval(()=>{if(settled){clearInterval(check);resolve();}},500);
  }));
  return result;
}
self.addEventListener('fetch', event=>{
  const url=new URL(event.request.url),direct=route(url);
  if(direct) { event.respondWith(serve(event,direct));return; }
  // Also translate root-relative same-origin assets requested by an SD document.
  if(event.clientId) event.respondWith((async()=>{
    const source=await self.clients.get(event.clientId),base=source && route(new URL(source.url));
    if(base && url.origin===self.location.origin) return serve(event,{...base,encoded:url.pathname.slice(1)});
    return fetch(event.request);
  })());
});
