/* QR entry point: connect to one device and display its SD website. */
'use strict';
(() => {
  const $=id=>document.getElementById(id);
  const language=(navigator.languages?.[0] || navigator.language || 'en');
  const locale=/^ja(?:-|$)/i.test(language)?'ja':'en';
  const messages={
    ja:{title:'ビューアー',content:'コンテンツ',loading:'読み込み中…',retry:'再接続する',
      loadFailed:'読み込みに失敗しました。もう一度お試しください。',
      advice:'通信環境を確認して、もう一度お試しください。',
      scanQr:'デバイスのQRコードを読み取ってアクセスしてください。',
      libraryMissing:'接続ライブラリを読み込めません。ページを再読み込みしてください。',
      timeout:'接続がタイムアウトしました。',disconnected:'デバイスとの接続が終了しました。',
      dataFailed:'データ通信に失敗しました。',deviceMissing:'デバイスが見つかりません。',
      connectFailed:'接続を開始できませんでした。',signalingClosed:'接続サービスとの通信が終了しました。'},
    en:{title:'Viewer',content:'Content',loading:'Loading…',retry:'Reconnect',
      loadFailed:'Unable to load. Please try again.',
      advice:'Check your connection and try again.',
      scanQr:'Scan the device’s QR code to open this page.',
      libraryMissing:'Unable to load the connection library. Please reload this page.',
      timeout:'The connection timed out.',disconnected:'The device has disconnected.',
      dataFailed:'Data transfer failed.',deviceMissing:'The device could not be found.',
      connectFailed:'Unable to connect.',signalingClosed:'The connection service has disconnected.'}
  };
  const t=messages[locale];
  document.documentElement.lang=locale;
  document.title=t.title;
  $('retry').textContent=t.retry;
  $('viewer').title=t.content;
  const params=new URL(location.href).searchParams;
  const device=params.get('device') || '';
  const validDevice=/^[A-Za-z0-9][A-Za-z0-9_-]{0,78}$/.test(device);
  let session;
  const current=s=>session===s && !s.closed;
  const transfer=new SdViewer({
    send:text=>{
      if(!session?.connection?.open) throw new Error(t.disconnected);
      session.connection.send(text);
    },
    digest:async bytes=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),n=>n.toString(16).padStart(2,'0')).join(''),
    progress:percent=>{
      if(session) $('progress').textContent=`${percent}%`;
    },
    fail:(path,error)=>{
      if(path==='index.html' && session) {console.warn('Page load:',error.message);fail(session,t.loadFailed);}
      else console.warn('SD file:',path,error.message);
    }
  });

  function stop() {
    $('spinner').hidden=true;
    const old=session;session=undefined;
    if(old) { old.closed=true;clearTimeout(old.timer); }
    transfer.cancel();
    old?.peer?.destroy();
    // Unload the old document so its resource requests cannot enter a retry.
    const frame=document.createElement('iframe');
    frame.id='viewer';frame.title=t.content;frame.hidden=true;
    $('viewer').replaceWith(frame);
  }
  function fail(s,message) {
    if(!current(s)) return;
    stop();
    document.title=t.title;
    $('connection').hidden=false;
    $('status').textContent=message;
    $('progress').textContent=t.advice;
    $('retry').hidden=false;
  }
  async function display(s) {
    clearTimeout(s.timer);
    $('status').textContent=t.loading;
    try {
      const url=await transfer.open();
      if(!current(s)) return;
      const frame=$('viewer');
      frame.onload=()=>{
        if(!current(s)) return;
        transfer.completeLoading();
        $('spinner').hidden=true;
        $('connection').hidden=true;
        document.title=frame.contentDocument?.title || t.title;
      };
      frame.src=url;frame.hidden=false;
    } catch(error) { console.warn('Page load:',error.message);fail(s,t.loadFailed); }
  }
  function start() {
    stop();
    document.title=t.title;
    $('connection').hidden=false;$('retry').hidden=true;$('progress').textContent='0%';
    if(!validDevice || (params.has('role') && params.get('role')!=='esp')) {
      $('status').textContent=t.scanQr;return;
    }
    const s={closed:false};session=s;
    $('spinner').hidden=false;
    $('status').textContent=t.loading;
    try {
      if(typeof Peer!=='function') {fail(s,t.libraryMissing);return;}
      s.peer=new Peer({secure:true,config:{iceServers:[{urls:'stun:stun.l.google.com:19302'}]}});
      s.timer=setTimeout(()=>fail(s,t.timeout),60000);
      s.peer.on('open',()=>{
        if(!current(s)) return;
        const connection=s.peer.connect(device,{serialization:'raw',reliable:true,label:'sd-host'});
        s.connection=connection;
        connection.on('open',()=>{if(current(s)) display(s);});
        connection.on('data',async data=>{
          if(!current(s)) return;
          const payload=typeof Blob!=='undefined' && data instanceof Blob?await data.arrayBuffer():data;
          if(current(s)) await transfer.receive(payload);
        });
        connection.on('close',()=>fail(s,t.disconnected));
        connection.on('error',()=>fail(s,t.dataFailed));
      });
      s.peer.on('error',error=>{
        if(s.connection?.open && ['network','socket-error','socket-closed'].includes(error.type)) return;
        fail(s,error.type==='peer-unavailable'?t.deviceMissing:t.connectFailed);
      });
      s.peer.on('disconnected',()=>{if(!s.connection?.open) fail(s,t.signalingClosed);});
    } catch(error) { console.warn('Connection:',error.message);fail(s,t.connectFailed); }
  }
  $('retry').onclick=start;
  window.addEventListener('pagehide',stop);
  start();
})();
