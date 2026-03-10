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

    // --- Stats view state ---
    let statsViewDirty = true;

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

    // --- DOM refs: stats view ---
    const statsView = document.getElementById('view-stats');
    const sankeySvg = document.getElementById('sankey-svg');
    const SVG_NS = 'http://www.w3.org/2000/svg';

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
            statsView.classList.toggle('active', tab === 'stats');
            if (tab === 'top') topViewDirty = true;
            if (tab === 'stats') statsViewDirty = true;
            scheduleRender();
        });
    }

    // Periodic refresh (ages and top data).
    setInterval(function() { scheduleRender(); }, 5000);

    // ========================================================================
    // Resizable & reorderable columns
    // ========================================================================
    // A dynamic <style> element overrides column widths and order. Both header
    // spans and virtual-scrolled row spans pick up the rules automatically.
    var colStyleEl = document.createElement('style');
    document.head.appendChild(colStyleEl);
    var colWidths = {};  // className -> width in px

    // Column order arrays — index = CSS order value.
    var feedColOrder = ['col-hash', 'col-status', 'col-peer', 'col-block', 'col-age', 'col-error', 'col-progress'];
    var topColOrder = ['top-col-hash', 'top-col-status', 'top-col-from', 'top-col-nonce', 'top-col-value', 'top-col-feecap', 'top-col-tipcap', 'top-col-gas', 'top-col-age', 'top-col-progress'];

    // Extract the column class (col-* or top-col-*) from an element.
    function getColClass(el) {
        return el.className.split(/\s+/).find(function(c) {
            return (c.startsWith('col-') || c.startsWith('top-col-')) && c !== 'col-resize';
        });
    }

    function applyColStyles() {
        var rules = [];
        for (var cls in colWidths) {
            rules.push('.' + cls + ' { width: ' + colWidths[cls] + 'px !important; flex: none !important; }');
        }
        // Emit order rules for both views.
        var orders = [feedColOrder, topColOrder];
        for (var v = 0; v < orders.length; v++) {
            for (var i = 0; i < orders[v].length; i++) {
                rules.push('.' + orders[v][i] + ' { order: ' + i + '; }');
            }
        }
        colStyleEl.textContent = rules.join('\n');
    }

    applyColStyles();

    // Add resize handles to all header columns.
    function setupResizeHandles(header) {
        var spans = header.querySelectorAll(':scope > span');
        for (var i = 0; i < spans.length; i++) {
            var handle = document.createElement('span');
            handle.className = 'col-resize';
            spans[i].appendChild(handle);
        }
    }

    // Find the feed and top headers and set up handles.
    var feedHeader = document.querySelector('#view-feed .table-header');
    setupResizeHandles(feedHeader);
    setupResizeHandles(topTableHeader);

    // --- Column resize (drag on handle) ---
    var resizeCol = null;
    var resizeStartX = 0;
    var resizeStartW = 0;
    var resizeHandle = null;

    document.addEventListener('mousedown', function(e) {
        if (!e.target.classList.contains('col-resize')) return;
        e.preventDefault();
        resizeHandle = e.target;
        resizeCol = e.target.parentElement;
        resizeStartX = e.clientX;
        resizeStartW = resizeCol.getBoundingClientRect().width;
        resizeHandle.classList.add('active');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
    });

    document.addEventListener('mousemove', function(e) {
        if (!resizeCol) return;
        var newW = Math.max(40, resizeStartW + (e.clientX - resizeStartX));
        var cls = getColClass(resizeCol);
        if (cls) {
            colWidths[cls] = Math.round(newW);
            applyColStyles();
        }
    });

    document.addEventListener('mouseup', function() {
        if (!resizeCol) return;
        if (resizeHandle) resizeHandle.classList.remove('active');
        resizeCol = null;
        resizeHandle = null;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
    });

    // --- Column reorder (drag-and-drop on header spans) ---
    function setupColumnDrag(header, orderArray) {
        var spans = header.querySelectorAll(':scope > span');
        for (var i = 0; i < spans.length; i++) {
            spans[i].draggable = true;
        }

        header.addEventListener('dragstart', function(e) {
            // Don't start drag from the resize handle.
            if (e.target.classList.contains('col-resize')) {
                e.preventDefault();
                return;
            }
            var span = e.target.closest('.table-header > span');
            if (!span) return;
            var cls = getColClass(span);
            if (!cls) return;
            e.dataTransfer.setData('text/plain', cls);
            e.dataTransfer.effectAllowed = 'move';
            span.classList.add('col-dragging');
        });

        header.addEventListener('dragend', function(e) {
            var span = e.target.closest('.table-header > span');
            if (span) span.classList.remove('col-dragging');
            // Clear all drop indicators.
            var all = header.querySelectorAll(':scope > span');
            for (var i = 0; i < all.length; i++) {
                all[i].classList.remove('col-drag-over');
            }
        });

        header.addEventListener('dragover', function(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            var span = e.target.closest('.table-header > span');
            if (!span) return;
            // Highlight only the hovered column.
            var all = header.querySelectorAll(':scope > span');
            for (var i = 0; i < all.length; i++) {
                all[i].classList.remove('col-drag-over');
            }
            span.classList.add('col-drag-over');
        });

        header.addEventListener('dragleave', function(e) {
            var span = e.target.closest('.table-header > span');
            if (span) span.classList.remove('col-drag-over');
        });

        header.addEventListener('drop', function(e) {
            e.preventDefault();
            var draggedCls = e.dataTransfer.getData('text/plain');
            var span = e.target.closest('.table-header > span');
            if (!span) return;
            var targetCls = getColClass(span);
            if (!targetCls || draggedCls === targetCls) return;

            // Move dragged column to the target's position.
            var dragIdx = orderArray.indexOf(draggedCls);
            if (dragIdx < 0) return;
            orderArray.splice(dragIdx, 1);
            var targetIdx = orderArray.indexOf(targetCls);
            orderArray.splice(targetIdx, 0, draggedCls);

            applyColStyles();
            scheduleRender();
        });
    }

    setupColumnDrag(feedHeader, feedColOrder);
    setupColumnDrag(topTableHeader, topColOrder);

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
        statsViewDirty = true;
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
        } else if (activeTab === 'top') {
            renderTopViewport();
        } else if (activeTab === 'stats') {
            renderStats();
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

    function closeDetail(panel) {
        panel.classList.remove('visible');
        selectedHash = null;
        scheduleRender();
    }

    detailPanel.querySelector('.detail-close').addEventListener('click', function() {
        closeDetail(detailPanel);
    });
    topDetailPanel.querySelector('.detail-close').addEventListener('click', function() {
        closeDetail(topDetailPanel);
    });

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
    // Stats view — Sankey diagram
    // ========================================================================
    var SANKEY_STAGES = ['announced', 'requested', 'received', 'pooled', 'included', 'finalized'];
    var SANKEY_LABELS = {
        announced: 'Announced', requested: 'Requested', received: 'Received',
        pooled: 'Pooled', included: 'Included', finalized: 'Finalized', rejected: 'Rejected'
    };
    var SANKEY_COLORS = {
        announced: '#666', requested: '#00838f', received: '#1565c0',
        pooled: '#f57f17', included: '#2e7d32', finalized: '#6a1b9a', rejected: '#c62828'
    };

    function svgEl(tag, attrs) {
        var el = document.createElementNS(SVG_NS, tag);
        if (attrs) {
            for (var k in attrs) {
                if (attrs.hasOwnProperty(k)) el.setAttribute(k, attrs[k]);
            }
        }
        return el;
    }

    // Create a filled curved band from (x1, y1a..y1b) to (x2, y2a..y2b).
    function sankeyPath(x1, y1a, y1b, x2, y2a, y2b) {
        var mx = (x1 + x2) / 2;
        return 'M' + x1 + ',' + y1a +
               ' C' + mx + ',' + y1a + ' ' + mx + ',' + y2a + ' ' + x2 + ',' + y2a +
               ' L' + x2 + ',' + y2b +
               ' C' + mx + ',' + y2b + ' ' + mx + ',' + y1b + ' ' + x1 + ',' + y1b +
               ' Z';
    }

    function renderStats() {
        if (!statsViewDirty) return;
        statsViewDirty = false;

        var svg = sankeySvg;
        var container = svg.parentElement;
        var W = container.clientWidth || 800;
        var H = container.clientHeight || 400;
        if (W < 10 || H < 10) return;

        while (svg.firstChild) svg.removeChild(svg.firstChild);
        svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);

        // Count transactions at each status.
        var counts = {};
        for (var s = 0; s < SANKEY_STAGES.length; s++) counts[SANKEY_STAGES[s]] = 0;
        counts.rejected = 0;
        txs.forEach(function(ev) {
            if (counts.hasOwnProperty(ev.newStatus)) counts[ev.newStatus]++;
        });
        var total = txs.size;

        if (total === 0) {
            var t = svgEl('text', {
                x: W / 2, y: H / 2, 'text-anchor': 'middle',
                fill: '#888', 'font-size': '14', 'font-family': 'inherit'
            });
            t.textContent = 'Waiting for transactions\u2026';
            svg.appendChild(t);
            return;
        }

        // Title.
        var title = svgEl('text', {
            x: W / 2, y: 22, 'text-anchor': 'middle',
            fill: '#888', 'font-size': '13', 'font-family': 'inherit'
        });
        title.textContent = 'Transaction Flow \u2014 ' + total.toLocaleString() + ' total';
        svg.appendChild(title);

        // Compute cumulative flow through each stage.
        // Each tx at status S implies it passed through all prior stages.
        // Rejected txs branch off from "received" (pool validation rejection).
        var nodeValues = [];  // total flow through each stage
        var linkValues = [];  // flow from stage[i] to stage[i+1]
        var remaining = total;
        for (var i = 0; i < SANKEY_STAGES.length; i++) {
            nodeValues.push(remaining);
            if (i === 2) remaining -= counts.rejected;  // branch to rejected
            remaining -= counts[SANKEY_STAGES[i]];
            if (i < SANKEY_STAGES.length - 1) {
                linkValues.push(Math.max(0, remaining));
            }
        }

        // Layout.
        var PAD = { top: 50, bottom: 50, left: 90, right: 70 };
        var NODE_W = 16;
        var mainH = H - PAD.bottom;  // reserve bottom for rejected
        var availW = W - PAD.left - PAD.right - NODE_W;
        var availH = mainH - PAD.top - 40; // extra room for labels
        var colGap = availW / (SANKEY_STAGES.length - 1);
        var scale = availH / Math.max(1, total);

        // Position main nodes (centered vertically in the available area).
        var nodes = [];
        for (var i = 0; i < SANKEY_STAGES.length; i++) {
            var h = Math.max(2, nodeValues[i] * scale);
            nodes.push({
                x: PAD.left + i * colGap,
                y: PAD.top + (availH - h) / 2,
                w: NODE_W,
                h: h,
                key: SANKEY_STAGES[i]
            });
        }

        // Draw links between consecutive main stages.
        for (var i = 0; i < linkValues.length; i++) {
            if (linkValues[i] <= 0) continue;
            var n0 = nodes[i], n1 = nodes[i + 1];
            var path = svgEl('path', {
                d: sankeyPath(n0.x + NODE_W, n0.y, n0.y + n1.h, n1.x, n1.y, n1.y + n1.h),
                fill: SANKEY_COLORS[SANKEY_STAGES[i]], opacity: '0.25'
            });
            svg.appendChild(path);
        }

        // Draw rejected branch (from received node).
        if (counts.rejected > 0) {
            var srcNode = nodes[2]; // received
            var rejH = Math.max(2, counts.rejected * scale);
            var linkSrcY = srcNode.y + linkValues[2] * scale; // below the pooled-flow portion

            // Position rejected node below the main flow, between received and pooled.
            var rejX = PAD.left + 2.5 * colGap;
            var rejY = srcNode.y + srcNode.h + 40;
            if (rejY + rejH > H - 20) rejY = H - 20 - rejH; // clamp to viewport

            // Link.
            var path = svgEl('path', {
                d: sankeyPath(srcNode.x + NODE_W, linkSrcY, linkSrcY + rejH, rejX, rejY, rejY + rejH),
                fill: SANKEY_COLORS.rejected, opacity: '0.25'
            });
            svg.appendChild(path);

            // Node.
            svg.appendChild(svgEl('rect', {
                x: rejX, y: rejY, width: NODE_W, height: rejH,
                fill: SANKEY_COLORS.rejected, rx: '2'
            }));

            // Label.
            var rl = svgEl('text', {
                x: rejX + NODE_W + 6, y: rejY + rejH / 2,
                'dominant-baseline': 'central', fill: '#e0e0e0',
                'font-size': '11', 'font-family': 'inherit'
            });
            rl.textContent = 'Rejected';
            svg.appendChild(rl);

            // Count.
            var rc = svgEl('text', {
                x: rejX + NODE_W + 6, y: rejY + rejH / 2 + 14,
                'dominant-baseline': 'central', fill: '#888',
                'font-size': '10', 'font-family': 'inherit'
            });
            rc.textContent = counts.rejected.toLocaleString();
            svg.appendChild(rc);
        }

        // Draw main nodes and labels.
        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];

            // Node rectangle.
            svg.appendChild(svgEl('rect', {
                x: n.x, y: n.y, width: n.w, height: Math.max(2, n.h),
                fill: SANKEY_COLORS[n.key], rx: '2'
            }));

            // Stage label above.
            var label = svgEl('text', {
                x: n.x + n.w / 2, y: n.y - 10,
                'text-anchor': 'middle', fill: '#e0e0e0',
                'font-size': '11', 'font-family': 'inherit'
            });
            label.textContent = SANKEY_LABELS[n.key];
            svg.appendChild(label);

            // Count at this stage below.
            var countLabel = svgEl('text', {
                x: n.x + n.w / 2, y: n.y + n.h + 16,
                'text-anchor': 'middle', fill: '#888',
                'font-size': '10', 'font-family': 'inherit'
            });
            countLabel.textContent = counts[n.key].toLocaleString();
            svg.appendChild(countLabel);
        }
    }

    // Re-render stats on window resize.
    window.addEventListener('resize', function() {
        if (activeTab === 'stats') { statsViewDirty = true; scheduleRender(); }
    });

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
