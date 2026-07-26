// Client for the direct (no-engine) chat example.
//
// The crypto is identical to the framework version's — same AES-GCM, same
// key-in-the-fragment design — because that part was never where the difficulty
// was. What differs is below the crypto: this client posts to endpoints that push
// targeted fragments, rather than triggering a re-derive.

(function () {
  var KEY = null;
  var TOKEN = null;

  // --- key handling -------------------------------------------------------

  function b64(bytes) {
    return btoa(String.fromCharCode.apply(null, new Uint8Array(bytes)))
      .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }

  function unb64(s) {
    var bin = atob(s.replace(/-/g, '+').replace(/_/g, '/'));
    var out = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  async function generateKey() {
    var k = await crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 },
                                            true, ['encrypt', 'decrypt']);
    return { key: k, b64: b64(await crypto.subtle.exportKey('raw', k)) };
  }

  async function importKey(s) {
    return crypto.subtle.importKey('raw', unb64(s), { name: 'AES-GCM' },
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
      // GCM is authenticated, so a tampered or wrong-key ciphertext fails rather
      // than decrypting to garbage.
      return '⚠ cannot decrypt';
    }
  }

  // --- decrypting what the server pushes ----------------------------------
  //
  // The server pushes message envelopes with the ciphertext in data attributes and
  // an empty body. A MutationObserver fills each one in as it arrives, so appended
  // and prepended messages are handled by the same path as the initial render.

  async function decryptAll() {
    if (!KEY) return;
    var nodes = document.querySelectorAll('.body[data-ct]:not([data-done])');
    for (var i = 0; i < nodes.length; i++) {
      var el = nodes[i];
      el.setAttribute('data-done', '1');
      el.textContent = await decrypt(el.getAttribute('data-ct'),
                                     el.getAttribute('data-iv'));
    }
  }

  var observer = new MutationObserver(function () { decryptAll(); });

  // --- posting ------------------------------------------------------------

  function post(path, body) {
    return fetch(path, { method: 'POST',
                         headers: { 'content-type': 'application/json' },
                         body: JSON.stringify(body) });
  }

  window.dSend = async function () {
    var input = document.getElementById('draft');
    if (!input || !input.value.trim()) return;
    var enc = await encrypt(input.value);
    input.value = '';
    await post('/send', { t: TOKEN, ct: enc.ct, iv: enc.iv });
  };

  var lastTyping = 0;
  window.dTyping = function () {
    // Throttled. The draft itself is never sent: the input owns its own value, so
    // there is nothing for the server to echo back and nothing to clobber.
    var now = Date.now();
    if (now - lastTyping > 800) {
      lastTyping = now;
      post('/typing', { t: TOKEN });
    }
  };

  function startHeartbeat() {
    var beat = function () {
      post('/heartbeat', { t: TOKEN, connId: window.__connId })
        .catch(function () { /* the next beat covers a missed one */ });
    };
    beat();
    setInterval(beat, 5000);
  }

  // --- create / join ------------------------------------------------------

  function showFieldError(inputId, message) {
    var input = document.getElementById(inputId);
    if (!input) return;
    var box = document.getElementById(inputId + '-error');
    if (!box) {
      box = document.createElement('p');
      box.id = inputId + '-error';
      box.className = 'field-error';
      input.insertAdjacentElement('afterend', box);
    }
    box.textContent = message;
    input.setAttribute('aria-invalid', 'true');
    input.focus();
    input.select();
  }

  window.dCreate = async function () {
    var name = document.getElementById('channel-name').value || 'untitled';
    var user = document.getElementById('username').value || 'anon';
    var k = await generateKey();
    var invite = (await (await post('/create', { name: name })).json()).invite;
    var res = await post('/join', { invite: invite, username: user });
    if (res.status === 409) {
      var b = await res.json().catch(function () { return {}; });
      showFieldError('username',
                     '"' + (b.username || user) + '" is already taken. Pick another.');
      return;
    }
    var token = (await res.json()).token;
    location.href = '/c/' + token + '#k=' + k.b64;
  };

  window.dJoin = async function (invite) {
    var user = document.getElementById('username').value || 'anon';
    var res = await post('/join', { invite: invite, username: user });
    if (res.status === 409) {
      var b = await res.json().catch(function () { return {}; });
      showFieldError('username',
                     '"' + (b.username || user) + '" is already taken in this ' +
                     'channel. Pick another name.');
      return;
    }
    if (!res.ok) { showFieldError('username', 'That invite is not valid.'); return; }
    var token = (await res.json()).token;
    // The key is in this page's fragment and must be carried across — the server
    // never sees it and could not forward it.
    location.href = '/c/' + token + location.hash;
  };

  // --- invite ------------------------------------------------------------

  window.dCopyInvite = async function (btn) {
    var label = btn.dataset.label || btn.textContent;
    btn.dataset.label = label;
    // The invite path plus THIS page's key. Deliberately not location.href, which is
    // a personal session URL — sharing that made every recipient the same user.
    var url = location.origin + btn.dataset.invite + location.hash;
    var box = document.getElementById('invite-fallback');
    var done = function (text, ok) {
      btn.textContent = text;
      btn.classList.toggle('is-copied', ok);
      setTimeout(function () {
        btn.textContent = label;
        btn.classList.remove('is-copied');
      }, 2000);
    };
    try {
      await navigator.clipboard.writeText(url);
      if (box) box.hidden = true;
      done('Copied', true);
      return;
    } catch (e) { /* fall through */ }
    if (box) {
      box.hidden = false;
      box.value = url;
      box.focus();
      box.select();
      done('Press ⌘C — link selected below', false);
    } else {
      done('Copy failed', false);
    }
  };

  // --- keyboard ----------------------------------------------------------

  function onEnter(inputId, fn) {
    var el = document.getElementById(inputId);
    if (!el) return;
    el.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') { e.preventDefault(); fn(); }
    });
  }

  window.dBindKeys = function (invite) {
    onEnter('channel-name', function () { window.dCreate(); });
    onEnter('username', invite ? function () { window.dJoin(invite); }
                               : function () { window.dCreate(); });
  };

  // --- boot ---------------------------------------------------------------

  window.dBoot = async function (token) {
    TOKEN = token;
    startHeartbeat();
    var m = location.hash.match(/k=([A-Za-z0-9_-]+)/);
    if (!m) {
      document.body.insertAdjacentHTML(
        'afterbegin',
        '<p class="banner banner--error">No key in the URL fragment. ' +
        'You need the full invite link to read this channel.</p>');
      return;
    }
    KEY = await importKey(m[1]);
    observer.observe(document.body, { childList: true, subtree: true });
    await decryptAll();
  };
})();
