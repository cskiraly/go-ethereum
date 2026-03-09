// txview - Transaction lifecycle viewer
// Uses virtual scrolling: only renders visible rows, keeping the DOM small
// regardless of how many transactions are tracked.
(function() {
    'use strict';

    // --- Constants ---
    const ROW_HEIGHT = 28;       // px per row (must match CSS)
    const OVERSCAN = 10;         // extra rows rendered above/below viewport
    const CACHE_STALE_MS = 5000; // re-fetch top cache entries after this

    // --- Shared state ---
    let ws = null;
    let rpcEndpoint = '';
    let rpcId = 1;
    const txs = new Map();       // hash -> event (from subscription)
    const pending = new Map();    // rpc id -> callback
    let selectedHash = null;
    let subId = null;
    let activeTab = 'feed';      // 'feed' or 'top'

    // --- Feed view state ---
    let visibleHashes = [];
    let feedViewDirty = true;
    let renderScheduled = false;

    // --- Top view state ---
    const topCache = new Map();      // hash -> { info: TxInfo, fetchedAt: ms }
    const topInflight = new Set();   // hashes being fetched
    let topVisibleHashes = [];
    let topViewDirty = true;
    let topSortKey = 'age';
    let topSortAsc = false;          // descending = oldest first for age

    // --- DOM refs: shared ---
    const statusDot = document.getElementById('status-dot');
    const statusText = document.getElementById('status-text');

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

    // --- DOM refs: feed view ---
    const feedView = document.getElementById('view-feed');
    const viewport = document.getElementById('viewport');
    const scrollSpacer = document.getElementById('scroll-spacer');
    const rowContainer = document.getElementById('row-container');
    const detailPanel = document.getElementById('detail-panel');
    const detailContent = document.getElementById('detail-content');
    const filterInput = document.getElementById('filter-input');
    const filterStatus = document.getElementById('filter-status');

    // --- DOM refs: top view ---
    const topView = document.getElementById('view-top');
    const topViewport = document.getElementById('top-viewport');
    const topScrollSpacer = document.getElementById('top-scroll-spacer');
    const topRowContainer = document.getElementById('top-row-container');
    const topDetailPanel = document.getElementById('top-detail-panel');
    const topDetailContent = document.getElementById('top-detail-content');
    const topFilterInput = document.getElementById('top-filter-input');
    const topFilterStatus = document.getElementById('top-filter-status');
    const topTableHeader = document.getElementById('top-table-header');

    // --- Init ---
    fetch('/config')
        .then(function(r) { return r.json(); })
        .then(function(cfg) { rpcEndpoint = cfg.rpcEndpoint; connect(); })
        .catch(function(err) { statusText.textContent = 'config error: ' + err; });

    // Feed view events.
    filterInput.addEventListener('input', function() { feedViewDirty = true; scheduleRender(); });
    filterStatus.addEventListener('change', function() { feedViewDirty = true; scheduleRender(); });
    viewport.addEventListener('scroll', scheduleRender);

    // Top view events.
    topFilterInput.addEventListener('input', function() { topViewDirty = true; scheduleRender(); });
    topFilterStatus.addEventListener('change', function() { topViewDirty = true; scheduleRender(); });
    topViewport.addEventListener('scroll', scheduleRender);

    // Sortable column headers in top view.
    topTableHeader.addEventListener('click', function(e) {
        var span = e.target.closest('.sortable');
        if (!span) return;
        var key = span.dataset.sort;
        if (topSortKey === key) {
            topSortAsc = !topSortAsc;
        } else {
            topSortKey = key;
            topSortAsc = false;
        }
        // Update header indicators.
        var sortables = topTableHeader.querySelectorAll('.sortable');
        for (var i = 0; i < sortables.length; i++) {
            sortables[i].classList.remove('active-sort', 'sort-asc', 'sort-desc');
        }
        span.classList.add('active-sort', topSortAsc ? 'sort-asc' : 'sort-desc');
        topViewDirty = true;
        scheduleRender();
    });

    // Tab switching.
    var tabs = document.querySelectorAll('.tab');
    for (var i = 0; i < tabs.length; i++) {
        tabs[i].addEventListener('click', function() {
            var tab = this.dataset.tab;
            if (tab === activeTab) return;
            activeTab = tab;
            for (var j = 0; j < tabs.length; j++) {
                tabs[j].classList.toggle('active', tabs[j].dataset.tab === tab);
            }
            feedView.classList.toggle('active', tab === 'feed');
            topView.classList.toggle('active', tab === 'top');
            if (tab === 'top') topViewDirty = true;
            scheduleRender();
        });
    }

    // Periodic refresh (ages and top data).
    setInterval(function() { scheduleRender(); }, 5000);

    // ========================================================================
    // WebSocket
    // ========================================================================
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
            var data;
            try { data = JSON.parse(msg.data); } catch(e) { return; }

            // Batch response (array of JSON-RPC results).
            if (Array.isArray(data)) {
                for (var i = 0; i < data.length; i++) {
                    dispatchResponse(data[i]);
                }
                return;
            }
            // Subscription event.
            if (data.method === 'txtracker_subscription' && data.params) {
                handleEvent(data.params.result);
                return;
            }
            // Single RPC response.
            dispatchResponse(data);
        };
    }

    function dispatchResponse(data) {
        if (data.id !== undefined && pending.has(data.id)) {
            var cb = pending.get(data.id);
            pending.delete(data.id);
            cb(data.result, data.error);
        }
    }

    function rpcCall(method, params, callback) {
        var id = rpcId++;
        if (callback) pending.set(id, callback);
        ws.send(JSON.stringify({jsonrpc: '2.0', id: id, method: method, params: params}));
    }

    // ========================================================================
    // Event handling (shared)
    // ========================================================================
    function handleEvent(ev) {
        if (!ev || !ev.txHash) return;
        var hash = ev.txHash;

        var old = txs.get(hash);
        if (old) {
            decrementCounter(old.newStatus);
        }

        ev._receivedAt = Date.now();
        txs.set(hash, ev);
        incrementCounter(ev.newStatus);
        counters.total.textContent = txs.size;

        // Invalidate top cache for this tx (status changed).
        topCache.delete(hash);

        feedViewDirty = true;
        topViewDirty = true;
        scheduleRender();
    }

    function incrementCounter(status) {
        var el = counters[status];
        if (el) el.textContent = parseInt(el.textContent) + 1;
    }

    function decrementCounter(status) {
        var el = counters[status];
        if (el) {
            var v = parseInt(el.textContent) - 1;
            el.textContent = v < 0 ? 0 : v;
        }
    }

    // ========================================================================
    // Render dispatch
    // ========================================================================
    function scheduleRender() {
        if (renderScheduled) return;
        renderScheduled = true;
        requestAnimationFrame(render);
    }

    function render() {
        renderScheduled = false;
        if (activeTab === 'feed') {
            renderFeedViewport();
        } else {
            renderTopViewport();
        }
    }

    // ========================================================================
    // Feed view
    // ========================================================================
    function matchesFeedFilter(hash, ev) {
        var status = filterStatus.value;
        if (status && ev.newStatus !== status) return false;
        var text = filterInput.value;
        if (text) {
            var lower = text.toLowerCase();
            if (!hash.toLowerCase().includes(lower) &&
                !(ev.peer || '').toLowerCase().includes(lower)) return false;
        }
        return true;
    }

    function rebuildFeedHashes() {
        var all = [];
        txs.forEach(function(ev, hash) {
            if (matchesFeedFilter(hash, ev)) all.push(hash);
        });
        visibleHashes = all.reverse();
        feedViewDirty = false;
    }

    function renderFeedViewport() {
        if (feedViewDirty) rebuildFeedHashes();

        var totalRows = visibleHashes.length;
        scrollSpacer.style.height = (totalRows * ROW_HEIGHT) + 'px';

        var scrollTop = viewport.scrollTop;
        var viewHeight = viewport.clientHeight;
        var firstVisible = Math.floor(scrollTop / ROW_HEIGHT);
        var lastVisible = Math.ceil((scrollTop + viewHeight) / ROW_HEIGHT);
        var startIdx = Math.max(0, firstVisible - OVERSCAN);
        var endIdx = Math.min(totalRows, lastVisible + OVERSCAN);

        rowContainer.style.transform = 'translateY(' + (startIdx * ROW_HEIGHT) + 'px)';

        var now = Date.now();
        var count = endIdx - startIdx;

        // Reuse existing row elements.
        while (rowContainer.children.length > count) {
            rowContainer.removeChild(rowContainer.lastChild);
        }
        while (rowContainer.children.length < count) {
            var row = document.createElement('div');
            row.className = 'vrow';
            row.innerHTML =
                '<span class="col-hash"></span>' +
                '<span class="col-status"></span>' +
                '<span class="col-peer"></span>' +
                '<span class="col-block"></span>' +
                '<span class="col-age"></span>' +
                '<span class="col-error"></span>' +
                '<span class="col-progress"></span>';
            rowContainer.appendChild(row);
        }

        var fetchNeeded = [];

        for (var i = 0; i < count; i++) {
            var idx = startIdx + i;
            var hash = visibleHashes[idx];
            var ev = txs.get(hash);
            var r = rowContainer.children[i];

            r.dataset.hash = hash;
            r.onclick = (function(h) { return function() { selectTx(h, 'feed'); }; })(hash);

            if (hash === selectedHash) {
                r.classList.add('selected');
            } else {
                r.classList.remove('selected');
            }

            var cols = r.children;
            cols[0].textContent = shortHash(hash);
            cols[0].title = hash;
            cols[1].textContent = ev.newStatus;
            cols[1].className = 'col-status badge badge-' + ev.newStatus;
            cols[2].textContent = ev.peer || '-';
            cols[3].textContent = ev.blockNum || '-';
            cols[4].textContent = ev._receivedAt ? timeSince(now - ev._receivedAt) : '-';
            cols[5].textContent = ev.rejectErr || '';

            var cached = topCache.get(hash);
            if (cached) {
                cols[6].innerHTML = renderProgress(cached.info);
            } else {
                cols[6].textContent = '\u2026';
                if (!topInflight.has(hash)) {
                    var entry = topCache.get(hash);
                    if (!entry || (now - entry.fetchedAt) > CACHE_STALE_MS) {
                        fetchNeeded.push(hash);
                    }
                }
            }
        }

        if (fetchNeeded.length > 0) {
            fetchTopBatch(fetchNeeded);
        }
    }

    // ========================================================================
    // Top view
    // ========================================================================
    function matchesTopFilter(hash, ev) {
        var status = topFilterStatus.value;
        if (status && ev.newStatus !== status) return false;
        var text = topFilterInput.value;
        if (text) {
            var lower = text.toLowerCase();
            var cached = topCache.get(hash);
            var from = cached ? (cached.info.From || '') : '';
            if (!hash.toLowerCase().includes(lower) &&
                !from.toLowerCase().includes(lower) &&
                !(ev.peer || '').toLowerCase().includes(lower)) return false;
        }
        return true;
    }

    function rebuildTopHashes() {
        var all = [];
        txs.forEach(function(ev, hash) {
            if (matchesTopFilter(hash, ev)) all.push(hash);
        });

        // Sort by the selected key. For age and status, use event data (available
        // for every tx) so sorting works across the full set. For RPC-dependent
        // keys, fall back to cached info (uncached rows go to bottom).
        all.sort(function(a, b) {
            var ea = txs.get(a), eb = txs.get(b);
            var cmp = 0;
            switch (topSortKey) {
                case 'age':
                    cmp = (ea._receivedAt || 0) - (eb._receivedAt || 0);
                    break;
                case 'status':
                    cmp = statusOrd(ea.newStatus) - statusOrd(eb.newStatus);
                    break;
                default:
                    var ca = topCache.get(a), cb = topCache.get(b);
                    if (!ca && !cb) return 0;
                    if (!ca) return 1;
                    if (!cb) return -1;
                    var ia = ca.info, ib = cb.info;
                    switch (topSortKey) {
                        case 'nonce':
                            cmp = (ia.Nonce || 0) - (ib.Nonce || 0);
                            break;
                        case 'gas':
                            cmp = (ia.Gas || 0) - (ib.Gas || 0);
                            break;
                        case 'value':
                            cmp = compareBigInt(ia.Value, ib.Value);
                            break;
                        case 'gasfeecap':
                            cmp = compareBigInt(ia.GasFeeCap, ib.GasFeeCap);
                            break;
                        case 'gastipcap':
                            cmp = compareBigInt(ia.GasTipCap, ib.GasTipCap);
                            break;
                    }
                    break;
            }
            return topSortAsc ? cmp : -cmp;
        });
        topVisibleHashes = all;
        topViewDirty = false;
    }

    function renderTopViewport() {
        if (topViewDirty) rebuildTopHashes();

        var totalRows = topVisibleHashes.length;
        topScrollSpacer.style.height = (totalRows * ROW_HEIGHT) + 'px';

        var scrollTop = topViewport.scrollTop;
        var viewHeight = topViewport.clientHeight;
        var firstVisible = Math.floor(scrollTop / ROW_HEIGHT);
        var lastVisible = Math.ceil((scrollTop + viewHeight) / ROW_HEIGHT);
        var startIdx = Math.max(0, firstVisible - OVERSCAN);
        var endIdx = Math.min(totalRows, lastVisible + OVERSCAN);

        topRowContainer.style.transform = 'translateY(' + (startIdx * ROW_HEIGHT) + 'px)';

        var now = Date.now();
        var count = endIdx - startIdx;

        // Reuse row elements.
        while (topRowContainer.children.length > count) {
            topRowContainer.removeChild(topRowContainer.lastChild);
        }
        while (topRowContainer.children.length < count) {
            var row = document.createElement('div');
            row.className = 'vrow';
            row.innerHTML =
                '<span class="top-col-hash"></span>' +
                '<span class="top-col-status"></span>' +
                '<span class="top-col-from"></span>' +
                '<span class="top-col-nonce"></span>' +
                '<span class="top-col-value"></span>' +
                '<span class="top-col-feecap"></span>' +
                '<span class="top-col-tipcap"></span>' +
                '<span class="top-col-gas"></span>' +
                '<span class="top-col-age"></span>' +
                '<span class="top-col-progress"></span>';
            topRowContainer.appendChild(row);
        }

        // Collect hashes that need fetching.
        var fetchNeeded = [];

        for (var i = 0; i < count; i++) {
            var idx = startIdx + i;
            var hash = topVisibleHashes[idx];
            var ev = txs.get(hash);
            var r = topRowContainer.children[i];
            var cached = topCache.get(hash);

            r.dataset.hash = hash;
            r.onclick = (function(h) { return function() { selectTx(h, 'top'); }; })(hash);

            if (hash === selectedHash) {
                r.classList.add('selected');
            } else {
                r.classList.remove('selected');
            }

            var cols = r.children;
            cols[0].textContent = shortHash(hash);
            cols[0].title = hash;

            // Status from event (always available).
            cols[1].textContent = ev.newStatus;
            cols[1].className = 'top-col-status badge badge-' + ev.newStatus;

            if (cached) {
                var info = cached.info;
                cols[2].textContent = shortAddr(info.From);
                cols[2].title = info.From || '';
                cols[3].textContent = info.Nonce || '0';
                cols[4].textContent = formatWei(info.Value);
                cols[5].textContent = formatGwei(info.GasFeeCap);
                cols[6].textContent = formatGwei(info.GasTipCap);
                cols[7].textContent = info.Gas ? formatNumber(info.Gas) : '-';
                cols[8].textContent = ev._receivedAt ? timeSince(now - ev._receivedAt) : '-';
                cols[9].innerHTML = renderProgress(info);
            } else {
                // Not yet fetched.
                cols[2].textContent = '\u2026';
                cols[3].textContent = '\u2026';
                cols[4].textContent = '\u2026';
                cols[5].textContent = '\u2026';
                cols[6].textContent = '\u2026';
                cols[7].textContent = '\u2026';
                cols[8].textContent = ev._receivedAt ? timeSince(now - ev._receivedAt) : '-';
                cols[9].textContent = '\u2026';

                // Need to fetch.
                if (!topInflight.has(hash)) {
                    var entry = topCache.get(hash);
                    if (!entry || (now - entry.fetchedAt) > CACHE_STALE_MS) {
                        fetchNeeded.push(hash);
                    }
                }
            }
        }

        // Batch-fetch visible rows that need data.
        if (fetchNeeded.length > 0) {
            fetchTopBatch(fetchNeeded);
        }
    }

    function fetchTopBatch(hashes) {
        if (!ws || ws.readyState !== WebSocket.OPEN) return;

        var batch = [];
        for (var i = 0; i < hashes.length; i++) {
            var h = hashes[i];
            topInflight.add(h);
            var id = rpcId++;
            batch.push({id: id, hash: h});
            // Register callback per id.
            (function(hash, reqId) {
                pending.set(reqId, function(result, err) {
                    topInflight.delete(hash);
                    if (!err && result) {
                        topCache.set(hash, {info: result, fetchedAt: Date.now()});
                    }
                    scheduleRender();
                });
            })(h, id);
        }

        // Send as JSON-RPC batch.
        var msgs = [];
        for (var j = 0; j < batch.length; j++) {
            msgs.push({jsonrpc: '2.0', id: batch[j].id, method: 'txtracker_getTx', params: [batch[j].hash]});
        }
        ws.send(JSON.stringify(msgs));
    }

    // ========================================================================
    // Detail panel (shared)
    // ========================================================================
    function selectTx(hash, view) {
        selectedHash = hash;
        scheduleRender();

        var panel = (view === 'top') ? topDetailPanel : detailPanel;
        var content = (view === 'top') ? topDetailContent : detailContent;
        panel.classList.add('visible');

        // If top view has cached info, render immediately.
        if (view === 'top') {
            var cached = topCache.get(hash);
            if (cached) {
                renderDetail(hash, cached.info, content);
                return;
            }
        }

        content.innerHTML = '<p>Loading\u2026</p>';
        rpcCall('txtracker_getTx', [hash], function(result, err) {
            if (err) {
                content.innerHTML = '<p>Error: ' + escapeHtml(JSON.stringify(err)) + '</p>';
                return;
            }
            if (!result) {
                content.innerHTML = '<p>Transaction not found in tracker</p>';
                return;
            }
            renderDetail(hash, result, content);
        });
    }

    function renderDetail(hash, info, container) {
        var base = parseTS(info.FirstSeen);
        var fields = [
            ['Hash', hash],
            ['Status', info.Status],
            ['Local', info.Local ? 'yes' : 'no'],
            ['Type', info.TxType],
            ['Size', info.TxSize + ' bytes'],
            ['From', info.From || '-'],
            ['Nonce', info.Nonce],
            ['Gas', info.Gas],
            ['Gas Fee Cap', info.GasFeeCap ? info.GasFeeCap + ' wei' : '-'],
            ['Gas Tip Cap', info.GasTipCap ? info.GasTipCap + ' wei' : '-'],
            ['Value', info.Value ? info.Value + ' wei' : '-'],
            ['To', info.To || '-'],
            ['First Seen', formatTS(info.FirstSeen, null)],
            ['Requested', formatTS(info.Requested, base)],
            ['Requested From', info.RequestedFrom || '-'],
            ['Received', formatTS(info.Received, base)],
            ['Pooled', formatTS(info.Pooled, base)],
            ['Included', formatTS(info.Included, base)],
            ['Finalized', formatTS(info.Finalized, base)],
            ['Deliverer', info.Deliverer || '-'],
            ['Announcers', (info.Announcers || []).join(', ') || '-'],
            ['Block #', info.BlockNum || '-'],
            ['Block Hash', info.BlockHash || '-'],
            ['Reject Error', info.RejectErr || '-'],
        ];

        var html = '';
        for (var i = 0; i < fields.length; i++) {
            html += '<div class="field"><label>' + escapeHtml(fields[i][0]) +
                    '</label><div class="value">' + escapeHtml(String(fields[i][1])) + '</div></div>';
        }
        container.innerHTML = html;
    }

    // ========================================================================
    // Progress rendering (shared by Feed and Top views)
    // ========================================================================
    function renderProgress(info) {
        var base = parseTS(info.FirstSeen);
        if (!base) return '<span class="top-loading">-</span>';
        var steps = [];

        steps.push('<span class="step step-announced">seen</span>');

        if (parseTS(info.Requested)) {
            steps.push('<span class="step step-requested">+' + msDelta(parseTS(info.Requested), base) + ' req</span>');
        }
        if (parseTS(info.Received)) {
            steps.push('<span class="step step-received">+' + msDelta(parseTS(info.Received), base) + ' rcv</span>');
        }
        if (parseTS(info.Pooled)) {
            steps.push('<span class="step step-pooled">+' + msDelta(parseTS(info.Pooled), base) + ' pool</span>');
        }
        if (parseTS(info.Included)) {
            steps.push('<span class="step step-included">+' + msDelta(parseTS(info.Included), base) + ' incl</span>');
        }
        if (parseTS(info.Finalized)) {
            steps.push('<span class="step step-finalized">+' + msDelta(parseTS(info.Finalized), base) + ' final</span>');
        }
        if (info.RejectErr) {
            steps.push('<span class="step step-rejected">rej</span>');
        }

        return '<span class="progress">' + steps.join('<span class="arrow">\u2192</span>') + '</span>';
    }

    // msDelta formats the millisecond difference between two Date objects.
    function msDelta(ts, base) {
        var deltaMs = ts.getTime() - base.getTime();
        if (deltaMs < 0) deltaMs = 0;
        var deltaSec = deltaMs / 1000;
        if (deltaSec < 0.1) return '0s';
        if (deltaSec < 10) return deltaSec.toFixed(1) + 's';
        if (deltaSec < 60) return Math.floor(deltaSec) + 's';
        if (deltaSec < 3600) return Math.floor(deltaSec / 60) + 'm' + Math.floor(deltaSec % 60) + 's';
        return Math.floor(deltaSec / 3600) + 'h' + Math.floor((deltaSec % 3600) / 60) + 'm';
    }

    // ========================================================================
    // Helpers
    // ========================================================================
    function shortHash(hash) {
        return hash.substring(0, 10) + '\u2026' + hash.substring(hash.length - 6);
    }

    function shortAddr(addr) {
        if (!addr || addr === '0x0000000000000000000000000000000000000000') return '-';
        return addr.substring(0, 8) + '\u2026' + addr.substring(addr.length - 4);
    }

    function timeSince(ms) {
        if (!ms || ms < 0) return '0s';
        var secs = Math.floor(ms / 1000);
        if (secs < 60) return secs + 's';
        if (secs < 3600) return Math.floor(secs / 60) + 'm ' + (secs % 60) + 's';
        return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm';
    }

    // parseTS parses an RFC3339 timestamp string into a Date, or null if empty/zero.
    function parseTS(s) {
        if (!s || s === '0001-01-01T00:00:00Z') return null;
        var d = new Date(s);
        return isNaN(d.getTime()) ? null : d;
    }

    // formatTS formats a timestamp as "HH:MM:SS.mmm (+Xs from base)" or "-".
    function formatTS(s, base) {
        var d = parseTS(s);
        if (!d) return '-';
        var abs = d.toLocaleTimeString('en-GB', {hour12: false}) + '.' +
                  String(d.getMilliseconds()).padStart(3, '0');
        if (base) {
            abs += ' (+' + msDelta(d, base) + ')';
        }
        return abs;
    }

    function formatWei(val) {
        if (!val) return '0';
        // Show in ETH if large enough.
        try {
            var n = BigInt(val);
            if (n === 0n) return '0';
            var eth = Number(n) / 1e18;
            if (eth >= 0.001) return eth.toFixed(4);
            var gwei = Number(n) / 1e9;
            if (gwei >= 1) return gwei.toFixed(1) + 'G';
            return val;
        } catch(e) { return val; }
    }

    function formatGwei(val) {
        if (!val) return '-';
        try {
            var n = Number(BigInt(val)) / 1e9;
            if (n < 0.01) return '<0.01';
            if (n < 100) return n.toFixed(2);
            return Math.floor(n).toString();
        } catch(e) { return val; }
    }

    function formatNumber(n) {
        if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
        if (n >= 1e3) return (n / 1e3).toFixed(0) + 'k';
        return String(n);
    }

    var STATUS_ORD = {announced:1, requested:2, received:3, pooled:4, included:5, finalized:6, rejected:7};
    function statusOrd(s) { return STATUS_ORD[s] || 0; }

    function compareBigInt(a, b) {
        try {
            var ba = BigInt(a || '0');
            var bb = BigInt(b || '0');
            if (ba < bb) return -1;
            if (ba > bb) return 1;
            return 0;
        } catch(e) { return 0; }
    }

    function escapeHtml(s) {
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }
})();
