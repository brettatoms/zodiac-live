// Client-side crypto and datastar glue for the chat example.
//
// The server never sees the key. It lives in the URL fragment (after #), which
// browsers do not transmit — so the link IS the credential. Anyone with the link
// can read the channel; there is no revocation and no per-user key. That is the
// tradeoff of the symmetric-key-in-fragment design.
//
// AES-GCM, 256-bit, random 12-byte IV per message. GCM is authenticated, so a
// tampered ciphertext fails to decrypt rather than decrypting to garbage.

(function () {
  var KEY = null;          // CryptoKey, once imported
  var CHANNEL = null;      // channel id, from the path
  var USERNAME = null;

  // --- key handling -------------------------------------------------------

  function b64(bytes) {
    return btoa(String.fromCharCode.apply(null, new Uint8Array(bytes)))
      .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }

  function unb64(s) {
    var t = s.replace(/-/g, '+').replace(/_/g, '/');
    var bin = atob(t);
    var out = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  async function generateKey() {
    var k = await crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 },
                                            true, ['encrypt', 'decrypt']);
    var raw = await crypto.subtle.exportKey('raw', k);
    return { key: k, b64: b64(raw) };
  }

  async function importKey(b64key) {
    return crypto.subtle.importKey('raw', unb64(b64key), { name: 'AES-GCM' },
                                   false, ['encrypt', 'decrypt']);
  }

  async function encrypt(text) {
    var iv = crypto.getRandomValues(new Uint8Array(12));
    var ct = await crypto.subtle.encrypt({ name: 'AES-GCM', iv: iv }, KEY,
                                         new TextEncoder().encode(text));
    return { ct: b64(ct), iv: b64(iv) };
  }

  async function decrypt(ctB64, ivB64) {
    try {
      var pt = await crypto.subtle.decrypt(
        { name: 'AES-GCM', iv: unb64(ivB64) }, KEY, unb64(ctB64));
      return new TextDecoder().decode(pt);
    } catch (e) {
      // Wrong key, or tampered ciphertext. GCM authenticates, so this is the
      // failure path rather than silent garbage.
      return '⚠ cannot decrypt';
    }
  }

  // --- decrypting rendered envelopes --------------------------------------
  //
  // The server renders <li data-ct=... data-iv=...> with an empty .body span.
  // This fills them in. Runs after every patch, because a patch can insert new
  // messages — and re-running on already-decrypted nodes is cheap since we mark
  // them done.

  async function decryptAll() {
    if (!KEY) return;
    var nodes = document.querySelectorAll('li.msg[data-ct]:not([data-done])');
    for (var i = 0; i < nodes.length; i++) {
      var li = nodes[i];
      var body = li.querySelector('.body');
      if (!body) continue;
      body.textContent = await decrypt(li.getAttribute('data-ct'),
                                       li.getAttribute('data-iv'));
      li.setAttribute('data-done', '1');
    }
  }

  // Datastar patches the DOM, so watch for new envelopes rather than decrypting
  // once at load. A MutationObserver is the only reliable hook: datastar does not
  // expose a post-patch callback.
  var observer = new MutationObserver(function () { decryptAll(); });

  // --- actions ------------------------------------------------------------

  var lastTyping = 0;

  window.chatTyping = function (text) {
    // Throttled: a keystroke per character would flood the bus, and coalescing
    // bounds the rebuilds but not the requests.
    var now = Date.now();
    if (now - lastTyping > 800) {
      lastTyping = now;
      // TWO calls, for two different kinds of state.
      //
      // /live/act updates the component's :draft, which is :recoverable and so
      // survives a deploy. It affects only this user's own view.
      //
      // /typing updates ephemeral server state and publishes a hint, which is what
      // makes OTHER users see "bob is typing...". Ephemeral because a hint carries
      // no data (§7.2) and "who is typing" is data — so it lives server-side and
      // the hint stays empty.
      var body = JSON.stringify({ liveId: window.__liveId, text: text });
      var opts = { method: 'POST',
                   headers: { 'content-type': 'application/json' },
                   body: body };
      fetch('/live/act?event=typing', opts);
      fetch('/typing', { method: 'POST',
                         headers: { 'content-type': 'application/json' },
                         body: body });
    }
  };

  window.chatSend = async function () {
    var input = document.querySelector('input[name=draft]');
    if (!input || !input.value.trim()) return;
    var enc = await encrypt(input.value);
    input.value = '';
    await fetch('/send', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        channel: CHANNEL, author: USERNAME, ct: enc.ct, iv: enc.iv
      })
    });
  };

  window.chatCreate = async function () {
    var name = document.getElementById('channel-name').value || 'untitled';
    var user = document.getElementById('username').value || 'anon';
    var k = await generateKey();
    var res = await fetch('/create', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: name })
    });
    var { id } = await res.json();
    // The key goes in the fragment, so it never reaches the server.
    location.href = '/c/' + id + '?user=' + encodeURIComponent(user) +
                    '#k=' + k.b64;
  };

  // --- boot ---------------------------------------------------------------

  window.chatBoot = async function (channelId, username) {
    CHANNEL = channelId;
    USERNAME = username;
    var m = location.hash.match(/k=([A-Za-z0-9_-]+)/);
    if (!m) {
      document.body.insertAdjacentHTML('afterbegin',
        '<p style="color:#b00">No key in the URL fragment. ' +
        'You need the full invite link to read this channel.</p>');
      return;
    }
    KEY = await importKey(m[1]);
    observer.observe(document.body, { childList: true, subtree: true });
    await decryptAll();
  };
})();
