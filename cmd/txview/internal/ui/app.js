// txview - Transaction lifecycle viewer
// Uses virtual scrolling: only renders visible rows, keeping the DOM small
// regardless of how many transactions are tracked.
(function() {
    'use strict';

    // --- Constants ---
    const ROW_HEIGHT = 28;       // px per row (must match CSS)
    const OVERSCAN = 10;         // extra rows rendered above/below viewport

    // --- State ---
    let ws = null;
    let rpcEndpoint = '';
    let rpcId = 1;
    const txs = new Map();       // hash -> event
    const pending = new Map();    // rpc id -> callback
    let selectedHash = null;
    let subId = null;

    // Filtered/sorted view: array of hashes in display order (newest first).
    let visibleHashes = [];
    let viewDirty = true;        // true when visibleHashes needs rebuild
    let renderScheduled = false;

    // --- DOM refs ---
    const statusDot = document.getElementById('status-dot');
    const statusText = document.getElementById('status-text');
    const viewport = document.getElementById('viewport');
    const scrollSpacer = document.getElementById('scroll-spacer');
    const rowContainer = document.getElementById('row-container');
    const detailPanel = document.getElementById('detail-panel');
    const detailContent = document.getElementById('detail-content');
    const filterInput = document.getElementById('filter-input');
    const filterStatus = document.getElementById('filter-status');

    const counters = {
        total: document.getElementById('cnt-total'),
        announced: document.getElementById('cnt-announced'),
        requested: document.getElementById('cnt-requested'),
        received: document.getElementById('cnt-received'),
        pooled: document.getElementById('cnt-pooled'),
        included: document.getElementById('cnt-included'),
        finalized: document.getElementById('cnt-finalized'),
        rejected: document.getElementById('cnt-rejected'),
    };

    // --- Init ---
    fetch('/config')
        .then(r => r.json())
        .then(cfg => { rpcEndpoint = cfg.rpcEndpoint; connect(); })
        .catch(err => { statusText.textContent = 'config error: ' + err; });

    filterInput.addEventListener('input', function() { viewDirty = true; scheduleRender(); });
    filterStatus.addEventListener('change', function() { viewDirty = true; scheduleRender(); });
    viewport.addEventListener('scroll', scheduleRender);

    // Update ages every 5 seconds.
    setInterval(function() { if (visibleHashes.length > 0) scheduleRender(); }, 5000);

    // --- WebSocket ---
    function connect() {
        console.log('[txview] connecting to', rpcEndpoint);
        ws = new WebSocket(rpcEndpoint);

        ws.onopen = function() {
            console.log('[txview] connected');
            statusDot.className = 'status-indicator connected';
            statusText.textContent = 'connected';
            rpcCall('txtracker_subscribe', ['events'], function(result, err) {
                if (err) { console.error('[txview] subscribe error:', err); return; }
                console.log('[txview] subscribed, id:', result);
                subId = result;
            });
        };

        ws.onclose = function(ev) {
            console.log('[txview] closed, code:', ev.code);
            statusDot.className = 'status-indicator disconnected';
            statusText.textContent = 'disconnected (code ' + ev.code + ')';
            subId = null;
            setTimeout(connect, 3000);
        };

        ws.onerror = function() { ws.close(); };

        ws.onmessage = function(msg) {
            let data;
            try { data = JSON.parse(msg.data); } catch(e) { return; }
            if (data.method === 'txtracker_subscription' && data.params) {
                handleEvent(data.params.result);
                return;
            }
            if (data.id !== undefined && pending.has(data.id)) {
                const cb = pending.get(data.id);
                pending.delete(data.id);
                cb(data.result, data.error);
            }
        };
    }

    function rpcCall(method, params, callback) {
        const id = rpcId++;
        if (callback) pending.set(id, callback);
        ws.send(JSON.stringify({jsonrpc: '2.0', id: id, method: method, params: params}));
    }

    // --- Event handling ---
    function handleEvent(ev) {
        if (!ev || !ev.txHash) return;
        const hash = ev.txHash;

        const old = txs.get(hash);
        if (old) {
            decrementCounter(old.newStatus);
        }

        ev._receivedAt = Date.now();
        txs.set(hash, ev);
        incrementCounter(ev.newStatus);
        counters.total.textContent = txs.size;

        viewDirty = true;
        scheduleRender();
    }

    function incrementCounter(status) {
        const el = counters[status];
        if (el) el.textContent = parseInt(el.textContent) + 1;
    }

    function decrementCounter(status) {
        const el = counters[status];
        if (el) {
            const v = parseInt(el.textContent) - 1;
            el.textContent = v < 0 ? 0 : v;
        }
    }

    // --- Filtering ---
    function matchesFilter(hash, ev) {
        const status = filterStatus.value;
        if (status && ev.newStatus !== status) return false;
        const text = filterInput.value;
        if (text) {
            const lower = text.toLowerCase();
            if (!hash.toLowerCase().includes(lower) &&
                !(ev.peer || '').toLowerCase().includes(lower)) return false;
        }
        return true;
    }

    function rebuildVisibleHashes() {
        visibleHashes = [];
        // Iterate in reverse insertion order (newest first).
        // Map preserves insertion order; collect then reverse.
        const all = [];
        txs.forEach(function(ev, hash) {
            if (matchesFilter(hash, ev)) all.push(hash);
        });
        // Reverse so newest (last inserted) is first.
        visibleHashes = all.reverse();
        viewDirty = false;
    }

    // --- Virtual scroll rendering ---
    function scheduleRender() {
        if (renderScheduled) return;
        renderScheduled = true;
        requestAnimationFrame(renderViewport);
    }

    function renderViewport() {
        renderScheduled = false;

        if (viewDirty) rebuildVisibleHashes();

        const totalRows = visibleHashes.length;
        const totalHeight = totalRows * ROW_HEIGHT;
        scrollSpacer.style.height = totalHeight + 'px';

        const scrollTop = viewport.scrollTop;
        const viewHeight = viewport.clientHeight;

        const firstVisible = Math.floor(scrollTop / ROW_HEIGHT);
        const lastVisible = Math.ceil((scrollTop + viewHeight) / ROW_HEIGHT);

        const startIdx = Math.max(0, firstVisible - OVERSCAN);
        const endIdx = Math.min(totalRows, lastVisible + OVERSCAN);

        // Position the row container at the correct offset.
        rowContainer.style.transform = 'translateY(' + (startIdx * ROW_HEIGHT) + 'px)';

        const now = Date.now();
        const count = endIdx - startIdx;

        // Reuse existing row elements where possible.
        while (rowContainer.children.length > count) {
            rowContainer.removeChild(rowContainer.lastChild);
        }
        while (rowContainer.children.length < count) {
            const row = document.createElement('div');
            row.className = 'vrow';
            row.innerHTML =
                '<span class="col-hash"></span>' +
                '<span class="col-status"></span>' +
                '<span class="col-peer"></span>' +
                '<span class="col-block"></span>' +
                '<span class="col-age"></span>' +
                '<span class="col-error"></span>';
            rowContainer.appendChild(row);
        }

        for (let i = 0; i < count; i++) {
            const idx = startIdx + i;
            const hash = visibleHashes[idx];
            const ev = txs.get(hash);
            const row = rowContainer.children[i];

            row.dataset.hash = hash;
            row.onclick = function() { selectTx(hash); };

            if (hash === selectedHash) {
                row.classList.add('selected');
            } else {
                row.classList.remove('selected');
            }

            const cols = row.children;
            const shortHash = hash.substring(0, 10) + '\u2026' + hash.substring(hash.length - 6);
            cols[0].textContent = shortHash;
            cols[0].title = hash;

            cols[1].textContent = ev.newStatus;
            cols[1].className = 'col-status badge badge-' + ev.newStatus;

            cols[2].textContent = ev.peer || '-';
            cols[3].textContent = ev.blockNum || '-';
            cols[4].textContent = ev._receivedAt ? timeSince(now - ev._receivedAt) : '-';
            cols[5].textContent = ev.rejectErr || '';
        }
    }

    // --- Detail panel ---
    function selectTx(hash) {
        selectedHash = hash;
        scheduleRender();

        detailPanel.classList.add('visible');
        detailContent.innerHTML = '<p>Loading\u2026</p>';

        rpcCall('txtracker_getTx', [hash], function(result, err) {
            if (err) {
                detailContent.innerHTML = '<p>Error: ' + escapeHtml(JSON.stringify(err)) + '</p>';
                return;
            }
            if (!result) {
                detailContent.innerHTML = '<p>Transaction not found in tracker</p>';
                return;
            }
            renderDetail(hash, result);
        });
    }

    function renderDetail(hash, info) {
        const fields = [
            ['Hash', hash],
            ['Status', info.Status],
            ['Local', info.Local ? 'yes' : 'no'],
            ['Type', info.TxType],
            ['Size', info.TxSize + ' bytes'],
            ['Nonce', info.Nonce],
            ['Gas', info.Gas],
            ['Gas Fee Cap', info.GasFeeCap ? info.GasFeeCap + ' wei' : '-'],
            ['Gas Tip Cap', info.GasTipCap ? info.GasTipCap + ' wei' : '-'],
            ['Value', info.Value ? info.Value + ' wei' : '-'],
            ['To', info.To || '-'],
            ['First Seen', formatTime(info.FirstSeen)],
            ['Requested', formatTime(info.Requested)],
            ['Requested From', info.RequestedFrom || '-'],
            ['Received', formatTime(info.Received)],
            ['Pooled', formatTime(info.Pooled)],
            ['Included', formatTime(info.Included)],
            ['Finalized', formatTime(info.Finalized)],
            ['Deliverer', info.Deliverer || '-'],
            ['Announcers', (info.Announcers || []).join(', ') || '-'],
            ['Block #', info.BlockNum || '-'],
            ['Block Hash', info.BlockHash || '-'],
            ['Reject Error', info.RejectErr || '-'],
        ];

        let html = '';
        for (const [label, value] of fields) {
            html += '<div class="field"><label>' + escapeHtml(label) +
                    '</label><div class="value">' + escapeHtml(String(value)) + '</div></div>';
        }
        detailContent.innerHTML = html;
    }

    // --- Helpers ---
    function timeSince(ms) {
        if (!ms || ms < 0) return '0s';
        const secs = Math.floor(ms / 1000);
        if (secs < 60) return secs + 's';
        if (secs < 3600) return Math.floor(secs / 60) + 'm ' + (secs % 60) + 's';
        return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm';
    }

    function formatTime(nanos) {
        if (!nanos) return '-';
        const secs = Math.floor(nanos / 1e9);
        if (secs < 60) return secs + 's uptime';
        if (secs < 3600) return Math.floor(secs / 60) + 'm ' + (secs % 60) + 's uptime';
        return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm uptime';
    }

    function escapeHtml(s) {
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }
})();
