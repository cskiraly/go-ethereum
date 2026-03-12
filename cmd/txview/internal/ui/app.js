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
    let cumRejected = 0;         // monotonic: total txs that ever entered rejected
    let cumDropped = 0;          // monotonic: total txs that ever entered dropped
    let cumFinalized = 0;        // monotonic: total txs that ever entered finalized

    // --- Sankey smoothing state ---
    let sankeySnapshots = [];        // ring buffer of periodic snapshots
    const SNAPSHOT_INTERVAL = 12000; // 12 seconds (one Ethereum block)
    const MAX_SNAPSHOTS = 7200;      // 24 hours at 12s intervals
    let sankeyHalfLife = 0;          // 0 = cumulative, >0 = half-life in seconds, Infinity = simple avg
    let sankeyRateMode = false;      // true when slider > 0
    let emaState = null;             // { prevFlows, ema, ts } or null (needs recompute)

    // --- Peers view state ---
    let peersData = {};           // peer -> PeerStats from RPC
    let peersSorted = [];         // sorted [{peer, stats}] for rendering
    let peersViewDirty = true;
    let peersSortKey = 'announced';
    let peersSortAsc = false;
    let peersLastFetch = 0;
    let peersTotalIncluded = 0;   // sum of Included across all peers
    let peersTotalFinalized = 0;  // sum of Finalized across all peers
    let selectedPeer = null;

    // --- Type filter state (shared across all panes) ---
    let typeFilter = new Set([0, 1, 2, 3, 4]);
    let showPrivate = true;        // show txs first seen in a block

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
        dropped: document.getElementById('cnt-dropped'),
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
    const evictionPanel = document.getElementById('eviction-panel');
    const evictionSummary = document.getElementById('eviction-summary');
    const evictionBars = document.getElementById('eviction-bars');

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

    // --- DOM refs: peers view ---
    const peersView = document.getElementById('view-peers');
    const peersViewport = document.getElementById('peers-viewport');
    const peersScrollSpacer = document.getElementById('peers-scroll-spacer');
    const peersRowContainer = document.getElementById('peers-row-container');
    const peersTableHeader = document.getElementById('peers-table-header');
    const peersDetailPanel = document.getElementById('peers-detail-panel');
    const peersDetailContent = document.getElementById('peers-detail-content');

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

    // Peers view events.
    peersViewport.addEventListener('scroll', scheduleRender);

    // Type filter helpers and events.
    function getCheckedTypes(containerId) {
        var s = new Set();
        var boxes = document.getElementById(containerId).querySelectorAll('input:checked');
        for (var i = 0; i < boxes.length; i++) s.add(parseInt(boxes[i].value));
        return s;
    }
    var typeFilterContainers = ['feed-type-filter', 'top-type-filter', 'stats-type-filter'];
    function syncTypeCheckboxes(sourceId) {
        typeFilterContainers.forEach(function(id) {
            if (id === sourceId) return;
            var boxes = document.getElementById(id).querySelectorAll('input[type="checkbox"]');
            for (var i = 0; i < boxes.length; i++) {
                boxes[i].checked = typeFilter.has(parseInt(boxes[i].value));
            }
        });
    }
    typeFilterContainers.forEach(function(id) {
        document.getElementById(id).addEventListener('change', function() {
            typeFilter = getCheckedTypes(id);
            syncTypeCheckboxes(id);
            feedViewDirty = true;
            topViewDirty = true;
            statsViewDirty = true;
            emaState = null; // invalidate EMA — filters changed
            scheduleRender();
        });
    });

    // Private transaction toggle (synced across all panes).
    var privateCbs = document.querySelectorAll('.private-cb');
    function syncPrivateCbs(source) {
        for (var i = 0; i < privateCbs.length; i++) {
            privateCbs[i].checked = showPrivate;
        }
    }
    for (var pi = 0; pi < privateCbs.length; pi++) {
        privateCbs[pi].addEventListener('change', function(e) {
            showPrivate = e.target.checked;
            syncPrivateCbs(e.target);
            feedViewDirty = true;
            topViewDirty = true;
            statsViewDirty = true;
            emaState = null; // invalidate EMA — filter changed
            scheduleRender();
        });
    }

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

    // Sortable column headers in peers view.
    peersTableHeader.addEventListener('click', function(e) {
        var span = e.target.closest('.sortable');
        if (!span) return;
        var key = span.dataset.sort;
        if (peersSortKey === key) {
            peersSortAsc = !peersSortAsc;
        } else {
            peersSortKey = key;
            peersSortAsc = false;
        }
        var sortables = peersTableHeader.querySelectorAll('.sortable');
        for (var i = 0; i < sortables.length; i++) {
            sortables[i].classList.remove('active-sort', 'sort-asc', 'sort-desc');
        }
        span.classList.add('active-sort', peersSortAsc ? 'sort-asc' : 'sort-desc');
        peersViewDirty = true;
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
            peersView.classList.toggle('active', tab === 'peers');
            if (tab === 'top') topViewDirty = true;
            if (tab === 'stats') { statsViewDirty = true; fetchEvictionStats(); }
            if (tab === 'peers') { peersViewDirty = true; fetchPeersData(); }
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
    var peersColOrder = ['peers-col-id', 'peers-col-announced', 'peers-col-delivered', 'peers-col-useful', 'peers-col-first', 'peers-col-included', 'peers-col-finalized', 'peers-col-useful-pct', 'peers-col-first-pct', 'peers-col-included-pct', 'peers-col-finalized-pct', 'peers-col-included-share', 'peers-col-finalized-share'];

    // Extract the column class (col-* or top-col-*) from an element.
    function getColClass(el) {
        return el.className.split(/\s+/).find(function(c) {
            return (c.startsWith('col-') || c.startsWith('top-col-') || c.startsWith('peers-col-')) && c !== 'col-resize';
        });
    }

    function applyColStyles() {
        var rules = [];
        for (var cls in colWidths) {
            rules.push('.' + cls + ' { width: ' + colWidths[cls] + 'px !important; flex: none !important; }');
        }
        // Emit order rules for both views.
        var orders = [feedColOrder, topColOrder, peersColOrder];
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
    setupResizeHandles(peersTableHeader);

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
    setupColumnDrag(peersTableHeader, peersColOrder);

    // ========================================================================
    // Resizable detail panel
    // ========================================================================
    var detailResizePanel = null;
    var detailResizeStartX = 0;
    var detailResizeStartW = 0;
    var detailResizeHandle = null;

    document.addEventListener('mousedown', function(e) {
        if (!e.target.classList.contains('detail-resize')) return;
        e.preventDefault();
        detailResizeHandle = e.target;
        detailResizePanel = e.target.closest('.detail-panel');
        detailResizeStartX = e.clientX;
        detailResizeStartW = detailResizePanel.getBoundingClientRect().width;
        detailResizeHandle.classList.add('active');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
    });

    document.addEventListener('mousemove', function(e) {
        if (!detailResizePanel) return;
        var newW = detailResizeStartW - (e.clientX - detailResizeStartX);
        newW = Math.max(200, Math.min(800, newW));
        detailResizePanel.style.width = newW + 'px';
    });

    document.addEventListener('mouseup', function() {
        if (!detailResizePanel) return;
        if (detailResizeHandle) detailResizeHandle.classList.remove('active');
        detailResizePanel = null;
        detailResizeHandle = null;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
    });

    // ========================================================================
    // Popout detail panel
    // ========================================================================
    detailPanel.querySelector('.detail-popout').addEventListener('click', function() {
        if (selectedHash) window.open('/tx/' + selectedHash, '_blank');
    });
    topDetailPanel.querySelector('.detail-popout').addEventListener('click', function() {
        if (selectedHash) window.open('/tx/' + selectedHash, '_blank');
    });

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
            ev._receivedAt = old._receivedAt;
            ev._firstStatus = old._firstStatus;
            ev._wasRequested = old._wasRequested || ev.newStatus === 'requested';
            ev._reorgCount = old._reorgCount || 0;
            ev._txType = ev.txType || old._txType || 0;
            ev.peer = ev.peer || old.peer;
            ev._dropReason = ev.dropReason || old._dropReason;
            ev._rejectErr = ev.rejectErr || old._rejectErr;
            // Track backward transition: included → pooled (reorg).
            if (old.newStatus === 'included' && ev.newStatus === 'pooled') {
                ev._reorgCount++;
            }
        } else {
            ev._receivedAt = Date.now();
            ev._firstStatus = ev.newStatus;
            ev._wasRequested = ev.newStatus === 'requested';
            ev._reorgCount = 0;
            ev._txType = ev.txType || 0;
            ev._dropReason = ev.dropReason || '';
            ev._rejectErr = ev.rejectErr || '';
        }
        txs.set(hash, ev);
        incrementCounter(ev.newStatus);
        counters.total.textContent = txs.size;

        // Track cumulative entries into sink states.
        if (ev.newStatus === 'rejected' && (!old || old.newStatus !== 'rejected')) cumRejected++;
        if (ev.newStatus === 'dropped' && (!old || old.newStatus !== 'dropped')) cumDropped++;
        if (ev.newStatus === 'finalized' && (!old || old.newStatus !== 'finalized')) cumFinalized++;

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
        } else if (activeTab === 'peers') {
            renderPeersViewport();
        }
    }

    // ========================================================================
    // Feed view
    // ========================================================================
    function isPrivateTx(ev) {
        return ev._firstStatus === 'included' || ev._firstStatus === 'finalized';
    }

    function matchesFeedFilter(hash, ev) {
        if (!typeFilter.has(ev._txType || 0)) return false;
        if (!showPrivate && isPrivateTx(ev)) return false;
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
            cols[5].textContent = ev.rejectErr || ev._dropReason || '';

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
        if (!typeFilter.has(ev._txType || 0)) return false;
        if (!showPrivate && isPrivateTx(ev)) return false;
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

    // --- Peer detail panel ---

    function selectPeer(peer) {
        selectedPeer = peer;
        scheduleRender();

        peersDetailPanel.classList.add('visible');

        var s = peersData[peer];
        if (!s) {
            peersDetailContent.innerHTML = '<p>Peer not found</p>';
            return;
        }
        renderPeerDetail(peer, s, peersDetailContent);
    }

    function closePeerDetail() {
        peersDetailPanel.classList.remove('visible');
        selectedPeer = null;
        scheduleRender();
    }

    peersDetailPanel.querySelector('.detail-close').addEventListener('click', function() {
        closePeerDetail();
    });

    function renderPeerDetail(peer, s, container) {
        var delivered = s.Delivered || 0;
        var announced = s.Announced || 0;
        var useful = s.UsefulDelivery || 0;
        var first = s.FirstAnnouncer || 0;
        var included = s.Included || 0;
        var finalized = s.Finalized || 0;

        var fields = [
            ['Peer ID', peer],
            ['Announced', formatNumber(announced)],
            ['Delivered', formatNumber(delivered)],
            ['Useful Deliveries', formatNumber(useful)],
            ['First Announcements', formatNumber(first)],
            ['Included', formatNumber(included)],
            ['Finalized', formatNumber(finalized)],
            ['Useful %', delivered ? (useful / delivered * 100).toFixed(1) + '%' : '-'],
            ['1st Announce %', announced ? (first / announced * 100).toFixed(1) + '%' : '-'],
            ['Included %', delivered ? (included / delivered * 100).toFixed(1) + '%' : '-'],
            ['Finalized %', delivered ? (finalized / delivered * 100).toFixed(1) + '%' : '-'],
            ['Included Share', peersTotalIncluded ? (included / peersTotalIncluded * 100).toFixed(1) + '%' : '-'],
            ['Finalized Share', peersTotalFinalized ? (finalized / peersTotalFinalized * 100).toFixed(1) + '%' : '-'],
        ];

        var html = '';
        for (var i = 0; i < fields.length; i++) {
            html += '<div class="field"><label>' + escapeHtml(fields[i][0]) +
                    '</label><div class="value">' + escapeHtml(String(fields[i][1])) + '</div></div>';
        }
        container.innerHTML = html;
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
            ['Dropped', formatTS(info.Dropped, base)],
            ['Drop Reason', info.DropReason || '-'],
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
        if (parseTS(info.Dropped)) {
            steps.push('<span class="step step-dropped">+' + msDelta(parseTS(info.Dropped), base) + ' drop</span>');
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
    // Peers view
    // ========================================================================
    function fetchPeersData() {
        if (!ws || ws.readyState !== WebSocket.OPEN) return;
        rpcCall('txtracker_getAllPeerStats', [], function(result, err) {
            if (err || !result) return;
            peersData = result;
            peersLastFetch = Date.now();
            peersViewDirty = true;
            if (selectedPeer && peersData[selectedPeer]) {
                renderPeerDetail(selectedPeer, peersData[selectedPeer], peersDetailContent);
            }
            scheduleRender();
        });
    }

    function rebuildPeersSorted() {
        var arr = [];
        for (var peer in peersData) {
            if (peersData.hasOwnProperty(peer)) {
                arr.push({peer: peer, stats: peersData[peer]});
            }
        }
        var totalIncl = 0, totalFinal = 0;
        for (var j = 0; j < arr.length; j++) {
            totalIncl += arr[j].stats.Included || 0;
            totalFinal += arr[j].stats.Finalized || 0;
        }
        peersTotalIncluded = totalIncl;
        peersTotalFinalized = totalFinal;

        arr.sort(function(a, b) {
            var sa = a.stats, sb = b.stats;
            var cmp = 0;
            switch (peersSortKey) {
                case 'announced':
                    cmp = (sa.Announced || 0) - (sb.Announced || 0);
                    break;
                case 'delivered':
                    cmp = (sa.Delivered || 0) - (sb.Delivered || 0);
                    break;
                case 'useful':
                    cmp = (sa.UsefulDelivery || 0) - (sb.UsefulDelivery || 0);
                    break;
                case 'first':
                    cmp = (sa.FirstAnnouncer || 0) - (sb.FirstAnnouncer || 0);
                    break;
                case 'included':
                    cmp = (sa.Included || 0) - (sb.Included || 0);
                    break;
                case 'finalized':
                    cmp = (sa.Finalized || 0) - (sb.Finalized || 0);
                    break;
                case 'useful-pct':
                    var ua = sa.Delivered ? (sa.UsefulDelivery || 0) / sa.Delivered : 0;
                    var ub = sb.Delivered ? (sb.UsefulDelivery || 0) / sb.Delivered : 0;
                    cmp = ua - ub;
                    break;
                case 'first-pct':
                    var fa = sa.Announced ? (sa.FirstAnnouncer || 0) / sa.Announced : 0;
                    var fb = sb.Announced ? (sb.FirstAnnouncer || 0) / sb.Announced : 0;
                    cmp = fa - fb;
                    break;
                case 'included-pct':
                    var ia = sa.Delivered ? (sa.Included || 0) / sa.Delivered : 0;
                    var ib = sb.Delivered ? (sb.Included || 0) / sb.Delivered : 0;
                    cmp = ia - ib;
                    break;
                case 'finalized-pct':
                    var fza = sa.Delivered ? (sa.Finalized || 0) / sa.Delivered : 0;
                    var fzb = sb.Delivered ? (sb.Finalized || 0) / sb.Delivered : 0;
                    cmp = fza - fzb;
                    break;
                case 'included-share':
                    var isa = peersTotalIncluded ? (sa.Included || 0) / peersTotalIncluded : 0;
                    var isb = peersTotalIncluded ? (sb.Included || 0) / peersTotalIncluded : 0;
                    cmp = isa - isb;
                    break;
                case 'finalized-share':
                    var fsa = peersTotalFinalized ? (sa.Finalized || 0) / peersTotalFinalized : 0;
                    var fsb = peersTotalFinalized ? (sb.Finalized || 0) / peersTotalFinalized : 0;
                    cmp = fsa - fsb;
                    break;
            }
            return peersSortAsc ? cmp : -cmp;
        });
        peersSorted = arr;
        peersViewDirty = false;
    }

    function renderPeersViewport() {
        if (peersViewDirty) rebuildPeersSorted();

        var totalRows = peersSorted.length;
        peersScrollSpacer.style.height = (totalRows * ROW_HEIGHT) + 'px';

        var scrollTop = peersViewport.scrollTop;
        var viewHeight = peersViewport.clientHeight;
        var firstVisible = Math.floor(scrollTop / ROW_HEIGHT);
        var lastVisible = Math.ceil((scrollTop + viewHeight) / ROW_HEIGHT);
        var startIdx = Math.max(0, firstVisible - OVERSCAN);
        var endIdx = Math.min(totalRows, lastVisible + OVERSCAN);

        peersRowContainer.style.transform = 'translateY(' + (startIdx * ROW_HEIGHT) + 'px)';

        var count = endIdx - startIdx;

        while (peersRowContainer.children.length > count) {
            peersRowContainer.removeChild(peersRowContainer.lastChild);
        }
        while (peersRowContainer.children.length < count) {
            var row = document.createElement('div');
            row.className = 'vrow';
            row.innerHTML =
                '<span class="peers-col-id"></span>' +
                '<span class="peers-col-announced"></span>' +
                '<span class="peers-col-delivered"></span>' +
                '<span class="peers-col-useful"></span>' +
                '<span class="peers-col-first"></span>' +
                '<span class="peers-col-included"></span>' +
                '<span class="peers-col-finalized"></span>' +
                '<span class="peers-col-useful-pct"></span>' +
                '<span class="peers-col-first-pct"></span>' +
                '<span class="peers-col-included-pct"></span>' +
                '<span class="peers-col-finalized-pct"></span>' +
                '<span class="peers-col-included-share"></span>' +
                '<span class="peers-col-finalized-share"></span>';
            peersRowContainer.appendChild(row);
        }

        for (var i = 0; i < count; i++) {
            var idx = startIdx + i;
            var entry = peersSorted[idx];
            var r = peersRowContainer.children[i];
            var s = entry.stats;

            var cols = r.children;
            cols[0].textContent = entry.peer;
            cols[0].title = entry.peer;
            cols[1].textContent = formatNumber(s.Announced || 0);
            cols[2].textContent = formatNumber(s.Delivered || 0);
            cols[3].textContent = formatNumber(s.UsefulDelivery || 0);
            cols[4].textContent = formatNumber(s.FirstAnnouncer || 0);
            cols[5].textContent = formatNumber(s.Included || 0);
            cols[6].textContent = formatNumber(s.Finalized || 0);
            cols[7].textContent = s.Delivered ? ((s.UsefulDelivery || 0) / s.Delivered * 100).toFixed(1) + '%' : '-';
            cols[8].textContent = s.Announced ? ((s.FirstAnnouncer || 0) / s.Announced * 100).toFixed(1) + '%' : '-';
            cols[9].textContent = s.Delivered ? ((s.Included || 0) / s.Delivered * 100).toFixed(1) + '%' : '-';
            cols[10].textContent = s.Delivered ? ((s.Finalized || 0) / s.Delivered * 100).toFixed(1) + '%' : '-';
            cols[11].textContent = peersTotalIncluded ? ((s.Included || 0) / peersTotalIncluded * 100).toFixed(1) + '%' : '-';
            cols[12].textContent = peersTotalFinalized ? ((s.Finalized || 0) / peersTotalFinalized * 100).toFixed(1) + '%' : '-';

            r.onclick = (function(p) { return function() { selectPeer(p); }; })(entry.peer);
            if (entry.peer === selectedPeer) {
                r.classList.add('selected');
            } else {
                r.classList.remove('selected');
            }
        }
    }

    // ========================================================================
    // Stats view — Sankey diagram
    // ========================================================================
    var SANKEY_COLORS = {
        announced: '#666', requested: '#00838f', received: '#1565c0',
        pooled: '#f57f17', included: '#2e7d32', finalized: '#6a1b9a',
        rejected: '#c62828', dropped: '#e65100', private: '#9c27b0',
        unsolicited: '#78909c'
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

    function drawNode(svg, x, y, w, h, color) {
        svg.appendChild(svgEl('rect', {
            x: x, y: y, width: w, height: Math.max(2, h),
            fill: color, rx: '2'
        }));
    }

    function drawLabel(svg, x, y, text, anchor, color, size) {
        var el = svgEl('text', {
            x: x, y: y, 'text-anchor': anchor || 'middle',
            'dominant-baseline': 'central',
            fill: color || '#e0e0e0', 'font-size': size || '11', 'font-family': 'inherit'
        });
        el.textContent = text;
        svg.appendChild(el);
    }

    function drawLink(svg, x1, y1, h1, x2, y2, h2, color, opacity) {
        if (h1 <= 0 && h2 <= 0) return;
        svg.appendChild(svgEl('path', {
            d: sankeyPath(x1, y1, y1 + Math.max(0.5, h1), x2, y2, y2 + Math.max(0.5, h2)),
            fill: color, opacity: opacity || '0.25'
        }));
    }

    // Draw a backward (right-to-left) link that arcs below the nodes.
    // srcNode/tgtNode: {x, y, h} — source is to the RIGHT, target to the LEFT.
    // bandH: visual thickness of the band.
    // dropY: how far below the nodes the arc should dip.
    function drawBackLink(svg, srcNode, tgtNode, nodeW, bandH, dropY, color) {
        if (bandH <= 0) return;
        var bh = Math.max(1.5, bandH);
        // Source exits from bottom-right of srcNode, target enters bottom-left of tgtNode.
        var x1 = srcNode.x;                    // left edge of source (link exits leftward)
        var x2 = tgtNode.x + nodeW;            // right edge of target (link enters from right)
        var y1 = srcNode.y + srcNode.h;         // bottom of source node
        var y2 = tgtNode.y + tgtNode.h;         // bottom of target node

        // Arc control points dip below.
        var cy = dropY;
        var mx = (x1 + x2) / 2;

        // Top edge of band (outer arc).
        var d = 'M' + x1 + ',' + y1 +
                ' C' + x1 + ',' + cy + ' ' + x2 + ',' + cy + ' ' + x2 + ',' + y2 +
                ' L' + x2 + ',' + (y2 + bh) +
                ' C' + x2 + ',' + (cy + bh) + ' ' + x1 + ',' + (cy + bh) + ' ' + x1 + ',' + (y1 + bh) +
                ' Z';
        svg.appendChild(svgEl('path', {
            d: d, fill: color, opacity: '0.3',
            'stroke': color, 'stroke-width': '1', 'stroke-dasharray': '4,3',
            'stroke-opacity': '0.6'
        }));
    }

    // Compute an unfiltered snapshot bucketed by (txType, isPrivate).
    // Filters are applied at render time via filterSnapshot().
    function computeRawSnapshot() {
        var buckets = {};
        txs.forEach(function(ev) {
            var type = ev._txType || 0;
            var priv = isPrivateTx(ev) ? 1 : 0;
            var key = type + ':' + priv;
            var b = buckets[key];
            if (!b) {
                b = buckets[key] = {
                    total: 0,
                    counts: { announced: 0, requested: 0, received: 0, pooled: 0, included: 0, finalized: 0, rejected: 0, dropped: 0 },
                    reqPath: { received: 0, pooled: 0, included: 0, finalized: 0, rejected: 0, dropped: 0 },
                    unsolPath: { received: 0, pooled: 0, included: 0, finalized: 0, rejected: 0, dropped: 0 },
                    privatePath: { included: 0, finalized: 0 },
                    nReorged: 0, dropReasons: {}, rejectReasons: {},
                    cumRejected: 0, cumDropped: 0, cumFinalized: 0
                };
            }
            b.total++;
            var s = ev.newStatus;
            if (b.counts.hasOwnProperty(s)) b.counts[s]++;
            b.nReorged += ev._reorgCount || 0;
            if (ev._dropReason) {
                b.dropReasons[ev._dropReason] = (b.dropReasons[ev._dropReason] || 0) + 1;
                b.cumDropped++;
            }
            if (ev._rejectErr) {
                var rk = normalizeRejectErr(ev._rejectErr);
                b.rejectReasons[rk] = (b.rejectReasons[rk] || 0) + 1;
                b.cumRejected++;
            }
            b.cumFinalized = b.counts.finalized;
            if (priv) {
                if (s === 'included') b.privatePath.included++;
                if (s === 'finalized') b.privatePath.finalized++;
                return;
            }
            if (s === 'announced' || s === 'requested') return;
            if (ev._wasRequested) {
                if (b.reqPath.hasOwnProperty(s)) b.reqPath[s]++;
            } else {
                if (b.unsolPath.hasOwnProperty(s)) b.unsolPath[s]++;
            }
        });
        return { ts: Date.now(), buckets: buckets };
    }

    // Aggregate matching buckets from a raw snapshot into a flat snapshot.
    function filterSnapshot(raw, tf, sp) {
        var r = {
            ts: raw.ts, total: 0,
            counts: { announced: 0, requested: 0, received: 0, pooled: 0, included: 0, finalized: 0, rejected: 0, dropped: 0 },
            reqPath: { received: 0, pooled: 0, included: 0, finalized: 0, rejected: 0, dropped: 0 },
            unsolPath: { received: 0, pooled: 0, included: 0, finalized: 0, rejected: 0, dropped: 0 },
            privatePath: { included: 0, finalized: 0 },
            nReorged: 0, dropReasons: {}, rejectReasons: {},
            cumRejected: 0, cumDropped: 0, cumFinalized: 0
        };
        for (var key in raw.buckets) {
            var parts = key.split(':');
            if (!tf.has(parseInt(parts[0]))) continue;
            if (!sp && parts[1] === '1') continue;
            var b = raw.buckets[key];
            r.total += b.total;
            r.nReorged += b.nReorged;
            r.cumRejected += b.cumRejected;
            r.cumDropped += b.cumDropped;
            r.cumFinalized += b.cumFinalized;
            var k;
            for (k in b.counts) r.counts[k] = (r.counts[k] || 0) + b.counts[k];
            for (k in b.reqPath) r.reqPath[k] = (r.reqPath[k] || 0) + b.reqPath[k];
            for (k in b.unsolPath) r.unsolPath[k] = (r.unsolPath[k] || 0) + b.unsolPath[k];
            for (k in b.privatePath) r.privatePath[k] = (r.privatePath[k] || 0) + b.privatePath[k];
            for (k in b.dropReasons) r.dropReasons[k] = (r.dropReasons[k] || 0) + b.dropReasons[k];
            for (k in b.rejectReasons) r.rejectReasons[k] = (r.rejectReasons[k] || 0) + b.rejectReasons[k];
        }
        return r;
    }

    // Convenience: compute a filtered snapshot from live data.
    function computeSankeySnapshot() {
        return filterSnapshot(computeRawSnapshot(), typeFilter, showPrivate);
    }

    // --- Rate-based EMA helpers ---

    // Extract cumulative flow values from a filtered snapshot into a flat object.
    function extractFlowValues(snap) {
        return {
            total: snap.total,
            announced: snap.counts.announced,
            requested: snap.counts.requested,
            received: snap.counts.received,
            pooled: snap.counts.pooled,
            included: snap.counts.included,
            finalized: snap.counts.finalized,
            rejected: snap.counts.rejected,
            dropped: snap.counts.dropped,
            reqPipeline: snap.counts.requested + snap.reqPath.received + snap.reqPath.pooled + snap.reqPath.included + snap.reqPath.finalized + snap.reqPath.rejected + snap.reqPath.dropped,
            unsolicited: snap.unsolPath.received + snap.unsolPath.pooled + snap.unsolPath.included + snap.unsolPath.finalized + snap.unsolPath.rejected + snap.unsolPath.dropped,
            reqToReceived: (snap.counts.requested + snap.reqPath.received + snap.reqPath.pooled + snap.reqPath.included + snap.reqPath.finalized + snap.reqPath.rejected + snap.reqPath.dropped) - snap.counts.requested,
            received_total: (snap.counts.requested + snap.reqPath.received + snap.reqPath.pooled + snap.reqPath.included + snap.reqPath.finalized + snap.reqPath.rejected + snap.reqPath.dropped) - snap.counts.requested + snap.unsolPath.received + snap.unsolPath.pooled + snap.unsolPath.included + snap.unsolPath.finalized + snap.unsolPath.rejected + snap.unsolPath.dropped,
            toPooled: snap.reqPath.pooled + snap.reqPath.included + snap.reqPath.finalized + snap.reqPath.dropped + snap.unsolPath.pooled + snap.unsolPath.included + snap.unsolPath.finalized + snap.unsolPath.dropped,
            nRejected: snap.reqPath.rejected + snap.unsolPath.rejected,
            nDropped: snap.reqPath.dropped + snap.unsolPath.dropped,
            private: snap.privatePath.included + snap.privatePath.finalized,
            nReorged: snap.nReorged,
            cumRejected: snap.cumRejected,
            cumDropped: snap.cumDropped,
            cumFinalized: snap.cumFinalized,
            // Per-reason maps (copy).
            dropReasons: Object.assign({}, snap.dropReasons),
            rejectReasons: Object.assign({}, snap.rejectReasons)
        };
    }

    // Slider value (0-100) → half-life in seconds. 0=cumulative, 100=Infinity (simple avg).
    function sliderToHalfLife(v) {
        if (v <= 0) return 0;
        if (v >= 100) return Infinity;
        return 12 * Math.pow(300, v / 100);
    }

    // Format half-life for display.
    function formatHalfLife(sec) {
        if (sec === 0) return 'cumulative';
        if (!isFinite(sec)) return '\u221E (avg)';
        if (sec < 60) return Math.round(sec) + 's';
        if (sec < 3600) return (sec / 60).toFixed(1).replace(/\.0$/, '') + 'm';
        return (sec / 3600).toFixed(1).replace(/\.0$/, '') + 'h';
    }

    // Format a per-second rate adaptively.
    function formatRate(r) {
        if (r >= 100) return Math.round(r).toString();
        if (r >= 1) return r.toFixed(1);
        if (r >= 0.01) return r.toFixed(2);
        return r.toFixed(3);
    }

    // O(1) incremental EMA update on a new raw snapshot.
    function updateEmaState(rawSnap, halfLife) {
        var filtered = filterSnapshot(rawSnap, typeFilter, showPrivate);
        var flows = extractFlowValues(filtered);
        var ts = rawSnap.ts;

        if (!emaState) {
            // First snapshot — no delta yet, just store flows for next time.
            emaState = { prevFlows: flows, ema: null, ts: ts };
            return;
        }

        var dt = (ts - emaState.ts) / 1000;
        if (dt <= 0) dt = SNAPSHOT_INTERVAL / 1000;
        var prev = emaState.prevFlows;

        // Compute per-second rates from deltas.
        var rates = {};
        var scalarKeys = ['total', 'announced', 'requested', 'received', 'pooled',
            'included', 'finalized', 'rejected', 'dropped', 'reqPipeline',
            'unsolicited', 'reqToReceived', 'received_total', 'toPooled',
            'nRejected', 'nDropped', 'private', 'nReorged', 'cumRejected',
            'cumDropped', 'cumFinalized'];
        for (var i = 0; i < scalarKeys.length; i++) {
            var k = scalarKeys[i];
            var delta = (flows[k] || 0) - (prev[k] || 0);
            rates[k] = Math.max(0, delta / dt);
        }
        // Per-reason rate maps.
        rates.dropReasons = computeReasonRates(flows.dropReasons, prev.dropReasons, dt);
        rates.rejectReasons = computeReasonRates(flows.rejectReasons, prev.rejectReasons, dt);

        // Blend into EMA.
        var decay = Math.pow(2, -dt / halfLife);
        if (!emaState.ema) {
            // Second snapshot — initialize EMA to first rate.
            emaState.ema = rates;
        } else {
            var ema = emaState.ema;
            for (var j = 0; j < scalarKeys.length; j++) {
                var sk = scalarKeys[j];
                ema[sk] = rates[sk] * (1 - decay) + (ema[sk] || 0) * decay;
            }
            blendEmaReasonMap(ema.dropReasons, rates.dropReasons, decay);
            blendEmaReasonMap(ema.rejectReasons, rates.rejectReasons, decay);
        }

        emaState.prevFlows = flows;
        emaState.ts = ts;
    }

    function computeReasonRates(curr, prev, dt) {
        var rates = {};
        for (var k in curr) {
            var delta = (curr[k] || 0) - (prev[k] || 0);
            rates[k] = Math.max(0, delta / dt);
        }
        return rates;
    }

    function blendEmaReasonMap(ema, rates, decay) {
        var k;
        // Blend existing keys.
        for (k in ema) {
            ema[k] = (rates[k] || 0) * (1 - decay) + ema[k] * decay;
            if (ema[k] < 1e-6) delete ema[k]; // prune near-zero
        }
        // Add new keys from rates.
        for (k in rates) {
            if (!(k in ema)) ema[k] = rates[k] * (1 - decay);
        }
    }

    // Replay all stored snapshots to rebuild EMA from scratch.
    function recomputeEmaState(halfLife) {
        emaState = null;
        for (var i = 0; i < sankeySnapshots.length; i++) {
            updateEmaState(sankeySnapshots[i], halfLife);
        }
    }

    // Simple average rates: (latest - first) / elapsed. For slider=100.
    function getSimpleAverageRates() {
        if (sankeySnapshots.length < 2) return null;
        var first = filterSnapshot(sankeySnapshots[0], typeFilter, showPrivate);
        var last = filterSnapshot(sankeySnapshots[sankeySnapshots.length - 1], typeFilter, showPrivate);
        var firstFlows = extractFlowValues(first);
        var lastFlows = extractFlowValues(last);
        var elapsed = (last.ts - first.ts) / 1000;
        if (elapsed <= 0) return null;

        var rates = {};
        var scalarKeys = ['total', 'announced', 'requested', 'received', 'pooled',
            'included', 'finalized', 'rejected', 'dropped', 'reqPipeline',
            'unsolicited', 'reqToReceived', 'received_total', 'toPooled',
            'nRejected', 'nDropped', 'private', 'nReorged', 'cumRejected',
            'cumDropped', 'cumFinalized'];
        for (var i = 0; i < scalarKeys.length; i++) {
            var k = scalarKeys[i];
            rates[k] = Math.max(0, ((lastFlows[k] || 0) - (firstFlows[k] || 0)) / elapsed);
        }
        rates.dropReasons = computeReasonRates(lastFlows.dropReasons, firstFlows.dropReasons, elapsed);
        rates.rejectReasons = computeReasonRates(lastFlows.rejectReasons, firstFlows.rejectReasons, elapsed);
        return rates;
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

        // -----------------------------------------------------------
        // Rate mode: use EMA or simple average rates.
        // -----------------------------------------------------------
        if (sankeyRateMode) {
            var rates = null;
            var titleSuffix = '';

            if (sankeyHalfLife === Infinity) {
                // Simple average (slider=100).
                rates = getSimpleAverageRates();
                titleSuffix = '(avg)';
            } else {
                // EMA (slider 1-99).
                if (!emaState || !emaState.ema) recomputeEmaState(sankeyHalfLife);
                if (emaState && emaState.ema) {
                    rates = emaState.ema;
                    titleSuffix = '(half-life: ' + formatHalfLife(sankeyHalfLife) + ')';
                }
            }

            if (!rates || sankeySnapshots.length < 2) {
                drawLabel(svg, W / 2, H / 2, 'Collecting rate data\u2026', 'middle', '#888', '14');
                return;
            }

            renderSankeyFromRates(svg, W, H, rates, titleSuffix);
            return;
        }

        // -----------------------------------------------------------
        // Cumulative mode (slider=0): existing behavior.
        // -----------------------------------------------------------
        var snap = computeSankeySnapshot();

        var total = snap.total;
        if (total === 0) {
            drawLabel(svg, W / 2, H / 2, 'Waiting for transactions\u2026', 'middle', '#888', '14');
            return;
        }

        var counts = snap.counts;
        var reqPath = snap.reqPath;
        var unsolPath = snap.unsolPath;
        var privatePath = snap.privatePath;
        var nReorged = snap.nReorged;
        var dropReasons = snap.dropReasons;
        var rejectReasons = snap.rejectReasons;

        var nPrivate = privatePath.included + privatePath.finalized;
        var nAnnounced = counts.announced;
        var nRequested = counts.requested;

        var reqTotal = nRequested + reqPath.received + reqPath.pooled + reqPath.included + reqPath.finalized + reqPath.rejected + reqPath.dropped;
        var unsolTotal = unsolPath.received + unsolPath.pooled + unsolPath.included + unsolPath.finalized + unsolPath.rejected + unsolPath.dropped;
        var announcedTotal = total - nPrivate;

        // Use snapshot's cumulative values.
        var snapCumRejected = snap.cumRejected;
        var snapCumDropped = snap.cumDropped;
        var snapCumFinalized = snap.cumFinalized;

        var atReceived = reqPath.received + unsolPath.received;
        var nRejected = reqPath.rejected + unsolPath.rejected;
        var nDropped = reqPath.dropped + unsolPath.dropped;
        var receivedTotal = reqTotal - nRequested + unsolTotal;  // all that reached received
        var toPooled = receivedTotal - atReceived - nRejected;
        var atPooled = reqPath.pooled + unsolPath.pooled;
        var toIncluded = toPooled - atPooled - nDropped;
        if (toIncluded < 0) toIncluded = 0;
        var includedTotal = toIncluded + nPrivate;
        var atIncluded = reqPath.included + unsolPath.included + privatePath.included;
        var toFinalized = includedTotal;  // everything included will finalize (conservation)

        // -----------------------------------------------------------
        // Layout
        // -----------------------------------------------------------
        var PAD = { top: 60, bottom: 72, left: 90, right: 70 };
        var NODE_W = 16;
        var availW = W - PAD.left - PAD.right - NODE_W;
        var availH = H - PAD.top - PAD.bottom - 40;
        var colGap = availW / 5;  // 6 main columns (0..5)
        var scale = availH / Math.max(1, announcedTotal || total);

        // Minimum visible height for nonzero flows.
        function sh(v) { return v > 0 ? Math.max(2, v * scale) : 0; }

        // Column x positions.
        function colX(c) { return PAD.left + c * colGap; }
        var yTop = PAD.top;

        // -----------------------------------------------------------
        // Position main nodes (top-aligned for smooth flow).
        // -----------------------------------------------------------
        var annH  = sh(announcedTotal);
        var reqH  = sh(reqTotal);
        var rcvH  = sh(receivedTotal);
        var poolH = sh(toPooled);
        var inclH = sh(includedTotal);
        var finH  = sh(toFinalized);

        var annNode  = { x: colX(0), y: yTop, h: annH };
        var reqNode  = { x: colX(1), y: yTop, h: reqH };
        var rcvNode  = { x: colX(2), y: yTop, h: rcvH };
        var poolNode = { x: colX(3), y: yTop, h: poolH };
        var inclNode = { x: colX(4), y: yTop, h: inclH };
        var finNode  = { x: colX(5), y: yTop, h: finH };

        // -----------------------------------------------------------
        // Draw links (back to front).
        // -----------------------------------------------------------
        // 1. Announced → Requested (top portion of announced).
        if (reqTotal > 0) {
            drawLink(svg, annNode.x + NODE_W, yTop, sh(reqTotal),
                     reqNode.x, yTop, reqH,
                     SANKEY_COLORS.requested);
        }

        // 2. Announced → Received (unsolicited, below requested flow).
        if (unsolTotal > 0) {
            var unsolSrcY = yTop + sh(reqTotal);
            var unsolTgtY = yTop + sh(reqTotal - nRequested); // below requested-path incoming
            drawLink(svg, annNode.x + NODE_W, unsolSrcY, sh(unsolTotal),
                     rcvNode.x, unsolTgtY, sh(unsolTotal),
                     SANKEY_COLORS.unsolicited);
        }

        // 3. Requested → Received (requested path that reached received).
        var reqToRcv = reqTotal - nRequested;
        if (reqToRcv > 0) {
            drawLink(svg, reqNode.x + NODE_W, yTop, sh(reqToRcv),
                     rcvNode.x, yTop, sh(reqToRcv),
                     SANKEY_COLORS.requested);
        }

        // 4. Received → Pooled.
        if (toPooled > 0) {
            drawLink(svg, rcvNode.x + NODE_W, yTop, sh(toPooled),
                     poolNode.x, yTop, poolH,
                     SANKEY_COLORS.received);
        }

        // 5. Pooled → Included.
        if (toIncluded > 0) {
            drawLink(svg, poolNode.x + NODE_W, yTop, sh(toIncluded),
                     inclNode.x, yTop, sh(toIncluded),
                     SANKEY_COLORS.pooled);
        }

        // 6. Included → Finalized.
        if (toFinalized > 0) {
            drawLink(svg, inclNode.x + NODE_W, yTop, sh(toFinalized),
                     finNode.x, yTop, finH,
                     SANKEY_COLORS.included);
        }

        // 7. Received → Rejected (branch down).
        if (nRejected > 0) {
            var rejSrcY = yTop + sh(toPooled);
            var rejBarH = sh(nRejected);
            var rejX = colX(2.5);
            var rejY = Math.max(rcvNode.y + rcvNode.h + 30, yTop + availH * 0.7);
            if (rejY + rejBarH > H - 30) rejY = H - 30 - rejBarH;

            drawLink(svg, rcvNode.x + NODE_W, rejSrcY, rejBarH,
                     rejX, rejY, rejBarH,
                     SANKEY_COLORS.rejected);
            // Rejected node.
            drawNode(svg, rejX, rejY, NODE_W, rejBarH, SANKEY_COLORS.rejected);
            drawLabel(svg, rejX + NODE_W + 6, rejY + rejBarH / 2 - 6,
                      'Rejected', 'start', '#e0e0e0');
            drawLabel(svg, rejX + NODE_W + 6, rejY + rejBarH / 2 + 8,
                      Math.round(snapCumRejected).toLocaleString() + ' total', 'start', '#888', '10');
            if (Math.round(counts.rejected) !== Math.round(snapCumRejected)) {
                drawLabel(svg, rejX + NODE_W + 6, rejY + rejBarH / 2 + 20,
                          Math.round(counts.rejected).toLocaleString() + ' now', 'start', '#555', '9');
            }
            // Show reject reason breakdown (one line per reason).
            var rejReasonKeys = Object.keys(rejectReasons).sort(function(a, b) { return rejectReasons[b] - rejectReasons[a]; });
            if (rejReasonKeys.length > 0) {
                var rejBaseY = rejY + rejBarH / 2 + (Math.round(counts.rejected) !== Math.round(snapCumRejected) ? 32 : 20);
                for (var ri = 0; ri < rejReasonKeys.length; ri++) {
                    drawLabel(svg, rejX + NODE_W + 6, rejBaseY + ri * 12,
                              Math.round(rejectReasons[rejReasonKeys[ri]]) + ' ' + rejReasonKeys[ri], 'start', '#777', '9');
                }
            }
        }

        // 8. Pooled → Dropped (branch down).
        if (nDropped > 0) {
            var dropSrcY = yTop + sh(toIncluded);
            var dropBarH = sh(nDropped);
            var dropX = colX(3.5);
            var dropY = Math.max(poolNode.y + poolNode.h + 30, yTop + availH * 0.55);
            if (dropY + dropBarH > H - 30) dropY = H - 30 - dropBarH;

            drawLink(svg, poolNode.x + NODE_W, dropSrcY, dropBarH,
                     dropX, dropY, dropBarH,
                     SANKEY_COLORS.dropped);
            // Dropped node.
            drawNode(svg, dropX, dropY, NODE_W, dropBarH, SANKEY_COLORS.dropped);
            drawLabel(svg, dropX + NODE_W + 6, dropY + dropBarH / 2 - 6,
                      'Dropped', 'start', '#e0e0e0');
            drawLabel(svg, dropX + NODE_W + 6, dropY + dropBarH / 2 + 8,
                      Math.round(snapCumDropped).toLocaleString() + ' total', 'start', '#888', '10');
            if (Math.round(counts.dropped) !== Math.round(snapCumDropped)) {
                drawLabel(svg, dropX + NODE_W + 6, dropY + dropBarH / 2 + 20,
                          Math.round(counts.dropped).toLocaleString() + ' now', 'start', '#555', '9');
            }
            // Show drop reason breakdown (one line per reason).
            var reasonKeys = Object.keys(dropReasons).sort(function(a, b) { return dropReasons[b] - dropReasons[a]; });
            if (reasonKeys.length > 0) {
                var baseY = dropY + dropBarH / 2 + (Math.round(counts.dropped) !== Math.round(snapCumDropped) ? 32 : 20);
                for (var ri = 0; ri < reasonKeys.length; ri++) {
                    drawLabel(svg, dropX + NODE_W + 6, baseY + ri * 12,
                              Math.round(dropReasons[reasonKeys[ri]]) + ' ' + reasonKeys[ri], 'start', '#777', '9');
                }
            }
        }

        // 9. Private → Included (from below-left, feeding into included node).
        if (nPrivate > 0) {
            var privBarH = sh(nPrivate);
            var privX = colX(3.9);
            var privY = Math.max(inclNode.y + inclNode.h + 30, yTop + availH * 0.7);
            if (privY + privBarH > H - 30) privY = H - 30 - privBarH;

            var privTgtY = yTop + sh(toIncluded); // enters below pooled-flow at included
            drawLink(svg, privX + NODE_W, privY, privBarH,
                     inclNode.x, privTgtY, privBarH,
                     SANKEY_COLORS.private);
            // Private node.
            drawNode(svg, privX, privY, NODE_W, privBarH, SANKEY_COLORS.private);
            drawLabel(svg, privX - 6, privY + privBarH / 2 - 6,
                      'Private', 'end', '#e0e0e0');
            drawLabel(svg, privX - 6, privY + privBarH / 2 + 8,
                      Math.round(nPrivate).toLocaleString() + ' total', 'end', '#888', '10');
        }

        // -----------------------------------------------------------
        // Draw main nodes and labels.
        // -----------------------------------------------------------
        // Cumulative = all txs that ever reached this state (current + moved on).
        var cumAnnounced  = announcedTotal;
        var cumRequested  = reqTotal;
        var cumReceived   = receivedTotal;
        var cumPooled     = toPooled;  // all that entered pooled
        var cumIncluded   = includedTotal;
        var mainNodes = [
            { n: annNode,  key: 'announced',  label: 'Announced',  now: nAnnounced,       cum: cumAnnounced },
            { n: reqNode,  key: 'requested',  label: 'Requested',  now: nRequested,        cum: cumRequested },
            { n: rcvNode,  key: 'received',   label: 'Received',   now: atReceived,        cum: cumReceived },
            { n: poolNode, key: 'pooled',     label: 'Pooled',     now: atPooled,          cum: cumPooled },
            { n: inclNode, key: 'included',   label: 'Included',   now: atIncluded,        cum: cumIncluded },
            { n: finNode,  key: 'finalized',  label: 'Finalized',  now: counts.finalized,  cum: snapCumFinalized },
        ];
        var pendingFinalization = atIncluded;  // txs at Included waiting for finalization

        for (var i = 0; i < mainNodes.length; i++) {
            var m = mainNodes[i];
            if (m.n.h <= 0) continue;
            drawNode(svg, m.n.x, m.n.y, NODE_W, m.n.h, SANKEY_COLORS[m.key]);
            drawLabel(svg, m.n.x + NODE_W / 2, m.n.y - 12, m.label, 'middle', '#e0e0e0');
            // Transit states: show current count as primary label.
            drawLabel(svg, m.n.x + NODE_W / 2, m.n.y + m.n.h + 14,
                      Math.round(m.now).toLocaleString() + ' now', 'middle', '#888', '10');
            if (Math.round(m.cum) !== Math.round(m.now)) {
                drawLabel(svg, m.n.x + NODE_W / 2, m.n.y + m.n.h + 26,
                          Math.round(m.cum).toLocaleString() + ' total', 'middle', '#555', '9');
            }
        }
        // Show pending finalization count below the Finalized node.
        if (pendingFinalization > 0 && finNode.h > 0) {
            var pendY = finNode.y + finNode.h + (Math.round(snapCumFinalized) !== Math.round(counts.finalized) ? 38 : 26);
            drawLabel(svg, finNode.x + NODE_W / 2, pendY,
                      Math.round(pendingFinalization).toLocaleString() + ' pending', 'middle', '#b0860a', '9');
        }

        // -----------------------------------------------------------
        // Backward link: Included → Pooled (reorgs).
        // -----------------------------------------------------------
        if (nReorged > 0) {
            var reorgBandH = sh(nReorged);
            // Arc dips below the lowest element.
            var maxBottom = Math.max(
                poolNode.y + poolNode.h,
                inclNode.y + inclNode.h
            );
            var reorgDropY = maxBottom + 40;
            // Clamp so it doesn't go past canvas.
            if (reorgDropY + reorgBandH + 20 > H) reorgDropY = H - reorgBandH - 20;
            drawBackLink(svg, inclNode, poolNode, NODE_W, reorgBandH, reorgDropY, '#ff6f00');
            // Label at the midpoint of the arc.
            var reorgMidX = (inclNode.x + poolNode.x + NODE_W) / 2;
            var reorgLabelY = reorgDropY + reorgBandH / 2 + 8;
            drawLabel(svg, reorgMidX, reorgLabelY,
                      nReorged.toLocaleString() + ' reorged', 'middle', '#ff6f00', '10');
        }

        // -----------------------------------------------------------
        // Annotation: unsolicited vs requested flow labels.
        // -----------------------------------------------------------
        if (reqTotal > 0 && unsolTotal > 0) {
            // Label on the requested path (above the flow).
            var midReqX = (annNode.x + NODE_W + reqNode.x) / 2;
            drawLabel(svg, midReqX, yTop - 4, 'requested', 'middle', '#00838f', '9');
            // Label on the unsolicited path (below requested label).
            var unsolLabelY = yTop + sh(reqTotal) + sh(unsolTotal) / 2;
            drawLabel(svg, midReqX, unsolLabelY, 'unsolicited', 'middle', '#78909c', '9');
        }

        // Title.
        drawLabel(svg, W / 2, 20,
                  'Transaction Flow \u2014 ' + Math.round(total).toLocaleString() + ' total',
                  'middle', '#888', '13');
    }

    // Render Sankey diagram from per-second rate values.
    function renderSankeyFromRates(svg, W, H, rates, titleSuffix) {
        var totalRate = rates.total || 0;
        if (totalRate < 1e-6) {
            drawLabel(svg, W / 2, H / 2, 'No flow detected\u2026', 'middle', '#888', '14');
            return;
        }

        var nPrivate = rates.private || 0;
        var nAnnounced = rates.announced || 0;
        var nRequested = rates.requested || 0;
        var reqTotal = rates.reqPipeline || 0;
        var unsolTotal = rates.unsolicited || 0;
        var announcedTotal = totalRate - nPrivate;

        var snapCumRejected = rates.cumRejected || 0;
        var snapCumDropped = rates.cumDropped || 0;
        var snapCumFinalized = rates.cumFinalized || 0;

        var atReceived = rates.received || 0; // currently at received (not meaningful for rates, use 0)
        var nRejected = rates.nRejected || 0;
        var nDropped = rates.nDropped || 0;
        var receivedTotal = rates.received_total || 0;
        var toPooled = rates.toPooled || 0;
        var atPooled = rates.pooled || 0; // currently at pooled
        var toIncluded = toPooled - nDropped;
        if (toIncluded < 0) toIncluded = 0;
        var includedTotal = toIncluded + nPrivate;
        var atIncluded = rates.included || 0;
        var toFinalized = includedTotal;
        var nReorged = rates.nReorged || 0;
        var dropReasons = rates.dropReasons || {};
        var rejectReasons = rates.rejectReasons || {};

        // --- Layout (same as cumulative mode) ---
        var PAD = { top: 60, bottom: 72, left: 90, right: 70 };
        var NODE_W = 16;
        var availW = W - PAD.left - PAD.right - NODE_W;
        var availH = H - PAD.top - PAD.bottom - 40;
        var colGap = availW / 5;
        var scale = availH / Math.max(1e-6, announcedTotal || totalRate);
        function sh(v) { return v > 0 ? Math.max(2, v * scale) : 0; }
        function colX(c) { return PAD.left + c * colGap; }
        var yTop = PAD.top;

        var annH  = sh(announcedTotal);
        var reqH  = sh(reqTotal);
        var rcvH  = sh(receivedTotal);
        var poolH = sh(toPooled);
        var inclH = sh(includedTotal);
        var finH  = sh(toFinalized);

        var annNode  = { x: colX(0), y: yTop, h: annH };
        var reqNode  = { x: colX(1), y: yTop, h: reqH };
        var rcvNode  = { x: colX(2), y: yTop, h: rcvH };
        var poolNode = { x: colX(3), y: yTop, h: poolH };
        var inclNode = { x: colX(4), y: yTop, h: inclH };
        var finNode  = { x: colX(5), y: yTop, h: finH };

        // --- Draw links ---
        if (reqTotal > 0)
            drawLink(svg, annNode.x + NODE_W, yTop, sh(reqTotal), reqNode.x, yTop, reqH, SANKEY_COLORS.requested);
        if (unsolTotal > 0) {
            var unsolSrcY = yTop + sh(reqTotal);
            var unsolTgtY = yTop + sh(reqTotal - nRequested);
            drawLink(svg, annNode.x + NODE_W, unsolSrcY, sh(unsolTotal), rcvNode.x, unsolTgtY, sh(unsolTotal), SANKEY_COLORS.unsolicited);
        }
        var reqToRcv = reqTotal - nRequested;
        if (reqToRcv > 0)
            drawLink(svg, reqNode.x + NODE_W, yTop, sh(reqToRcv), rcvNode.x, yTop, sh(reqToRcv), SANKEY_COLORS.requested);
        if (toPooled > 0)
            drawLink(svg, rcvNode.x + NODE_W, yTop, sh(toPooled), poolNode.x, yTop, poolH, SANKEY_COLORS.received);
        if (toIncluded > 0)
            drawLink(svg, poolNode.x + NODE_W, yTop, sh(toIncluded), inclNode.x, yTop, sh(toIncluded), SANKEY_COLORS.pooled);
        if (toFinalized > 0)
            drawLink(svg, inclNode.x + NODE_W, yTop, sh(toFinalized), finNode.x, yTop, finH, SANKEY_COLORS.included);

        // Rejected branch.
        if (nRejected > 0) {
            var rejBarH = sh(nRejected);
            var rejX = colX(2.5);
            var rejY = Math.max(rcvNode.y + rcvNode.h + 30, yTop + availH * 0.7);
            if (rejY + rejBarH > H - 30) rejY = H - 30 - rejBarH;
            drawLink(svg, rcvNode.x + NODE_W, yTop + sh(toPooled), rejBarH, rejX, rejY, rejBarH, SANKEY_COLORS.rejected);
            drawNode(svg, rejX, rejY, NODE_W, rejBarH, SANKEY_COLORS.rejected);
            drawLabel(svg, rejX + NODE_W + 6, rejY + rejBarH / 2 - 6, 'Rejected', 'start', '#e0e0e0');
            drawLabel(svg, rejX + NODE_W + 6, rejY + rejBarH / 2 + 8, formatRate(snapCumRejected) + '/s', 'start', '#888', '10');
            var rejReasonKeys = Object.keys(rejectReasons).sort(function(a, b) { return (rejectReasons[b] || 0) - (rejectReasons[a] || 0); });
            if (rejReasonKeys.length > 0) {
                var rejBaseY = rejY + rejBarH / 2 + 20;
                for (var ri = 0; ri < rejReasonKeys.length; ri++) {
                    drawLabel(svg, rejX + NODE_W + 6, rejBaseY + ri * 12,
                              formatRate(rejectReasons[rejReasonKeys[ri]]) + '/s ' + rejReasonKeys[ri], 'start', '#777', '9');
                }
            }
        }

        // Dropped branch.
        if (nDropped > 0) {
            var dropBarH = sh(nDropped);
            var dropX = colX(3.5);
            var dropY = Math.max(poolNode.y + poolNode.h + 30, yTop + availH * 0.55);
            if (dropY + dropBarH > H - 30) dropY = H - 30 - dropBarH;
            drawLink(svg, poolNode.x + NODE_W, yTop + sh(toIncluded), dropBarH, dropX, dropY, dropBarH, SANKEY_COLORS.dropped);
            drawNode(svg, dropX, dropY, NODE_W, dropBarH, SANKEY_COLORS.dropped);
            drawLabel(svg, dropX + NODE_W + 6, dropY + dropBarH / 2 - 6, 'Dropped', 'start', '#e0e0e0');
            drawLabel(svg, dropX + NODE_W + 6, dropY + dropBarH / 2 + 8, formatRate(snapCumDropped) + '/s', 'start', '#888', '10');
            var reasonKeys = Object.keys(dropReasons).sort(function(a, b) { return (dropReasons[b] || 0) - (dropReasons[a] || 0); });
            if (reasonKeys.length > 0) {
                var baseY = dropY + dropBarH / 2 + 20;
                for (var ri = 0; ri < reasonKeys.length; ri++) {
                    drawLabel(svg, dropX + NODE_W + 6, baseY + ri * 12,
                              formatRate(dropReasons[reasonKeys[ri]]) + '/s ' + reasonKeys[ri], 'start', '#777', '9');
                }
            }
        }

        // Private branch.
        if (nPrivate > 0) {
            var privBarH = sh(nPrivate);
            var privX = colX(3.9);
            var privY = Math.max(inclNode.y + inclNode.h + 30, yTop + availH * 0.7);
            if (privY + privBarH > H - 30) privY = H - 30 - privBarH;
            var privTgtY = yTop + sh(toIncluded);
            drawLink(svg, privX + NODE_W, privY, privBarH, inclNode.x, privTgtY, privBarH, SANKEY_COLORS.private);
            drawNode(svg, privX, privY, NODE_W, privBarH, SANKEY_COLORS.private);
            drawLabel(svg, privX - 6, privY + privBarH / 2 - 6, 'Private', 'end', '#e0e0e0');
            drawLabel(svg, privX - 6, privY + privBarH / 2 + 8, formatRate(nPrivate) + '/s', 'end', '#888', '10');
        }

        // --- Main nodes and labels (rate mode) ---
        var mainNodes = [
            { n: annNode,  key: 'announced',  label: 'Announced',  rate: announcedTotal },
            { n: reqNode,  key: 'requested',  label: 'Requested',  rate: reqTotal },
            { n: rcvNode,  key: 'received',   label: 'Received',   rate: receivedTotal },
            { n: poolNode, key: 'pooled',     label: 'Pooled',     rate: toPooled },
            { n: inclNode, key: 'included',   label: 'Included',   rate: includedTotal },
            { n: finNode,  key: 'finalized',  label: 'Finalized',  rate: toFinalized },
        ];
        for (var i = 0; i < mainNodes.length; i++) {
            var m = mainNodes[i];
            if (m.n.h <= 0) continue;
            drawNode(svg, m.n.x, m.n.y, NODE_W, m.n.h, SANKEY_COLORS[m.key]);
            drawLabel(svg, m.n.x + NODE_W / 2, m.n.y - 12, m.label, 'middle', '#e0e0e0');
            drawLabel(svg, m.n.x + NODE_W / 2, m.n.y + m.n.h + 14,
                      formatRate(m.rate) + '/s', 'middle', '#888', '10');
        }

        // Reorg backward link.
        if (nReorged > 0) {
            var reorgBandH = sh(nReorged);
            var maxBottom = Math.max(poolNode.y + poolNode.h, inclNode.y + inclNode.h);
            var reorgDropY = maxBottom + 40;
            if (reorgDropY + reorgBandH + 20 > H) reorgDropY = H - reorgBandH - 20;
            drawBackLink(svg, inclNode, poolNode, NODE_W, reorgBandH, reorgDropY, '#ff6f00');
            var reorgMidX = (inclNode.x + poolNode.x + NODE_W) / 2;
            drawLabel(svg, reorgMidX, reorgDropY + reorgBandH / 2 + 8,
                      formatRate(nReorged) + '/s reorged', 'middle', '#ff6f00', '10');
        }

        // Unsolicited vs requested labels.
        if (reqTotal > 0 && unsolTotal > 0) {
            var midReqX = (annNode.x + NODE_W + reqNode.x) / 2;
            drawLabel(svg, midReqX, yTop - 4, 'requested', 'middle', '#00838f', '9');
            var unsolLabelY = yTop + sh(reqTotal) + sh(unsolTotal) / 2;
            drawLabel(svg, midReqX, unsolLabelY, 'unsolicited', 'middle', '#78909c', '9');
        }

        // Title with rate.
        drawLabel(svg, W / 2, 20,
                  'Transaction Flow \u2014 ' + formatRate(totalRate) + ' tx/s ' + titleSuffix,
                  'middle', '#888', '13');
    }

    // Eviction stats — fetched periodically from the tracker.
    var lastEvictionStats = null;

    function fetchEvictionStats() {
        if (!ws || ws.readyState !== 1) return;
        rpcCall('txtracker_getStats', [], function(result, err) {
            if (err || !result) return;
            lastEvictionStats = result;
            renderEvictionPanel();
        });
    }

    function renderEvictionPanel() {
        var stats = lastEvictionStats;
        if (!stats || !stats.evicted) {
            evictionPanel.style.display = 'none';
            return;
        }
        var ev = stats.evicted;
        if (ev.total === 0) {
            evictionPanel.style.display = 'none';
            return;
        }
        evictionPanel.style.display = '';
        evictionSummary.textContent = ev.total.toLocaleString() + ' evicted (' +
            stats.total.toLocaleString() + ' / ' + stats.capacity.toLocaleString() + ' capacity)';

        var states = [
            { key: 'announced', label: 'Announced', color: SANKEY_COLORS.announced },
            { key: 'requested', label: 'Requested', color: SANKEY_COLORS.requested },
            { key: 'received',  label: 'Received',  color: SANKEY_COLORS.received },
            { key: 'pooled',    label: 'Pooled',    color: SANKEY_COLORS.pooled },
            { key: 'included',  label: 'Included',  color: SANKEY_COLORS.included },
            { key: 'finalized', label: 'Finalized', color: SANKEY_COLORS.finalized },
            { key: 'rejected',  label: 'Rejected',  color: SANKEY_COLORS.rejected },
            { key: 'dropped',   label: 'Dropped',   color: SANKEY_COLORS.dropped }
        ];
        evictionBars.innerHTML = '';
        for (var i = 0; i < states.length; i++) {
            var s = states[i];
            var count = ev[s.key] || 0;
            if (count === 0) continue;
            var pct = (count / ev.total) * 100;

            var bar = document.createElement('div');
            bar.className = 'eviction-bar';

            var fill = document.createElement('span');
            fill.className = 'bar-fill';
            fill.style.background = s.color;
            fill.style.width = Math.max(2, pct * 1.5) + 'px';
            bar.appendChild(fill);

            var label = document.createElement('span');
            label.className = 'bar-label';
            label.textContent = s.label + ':';
            bar.appendChild(label);

            var val = document.createElement('span');
            val.className = 'bar-count';
            val.textContent = count.toLocaleString();
            bar.appendChild(val);

            evictionBars.appendChild(bar);
        }
    }

    // Poll eviction stats every 5 seconds when on the stats tab.
    setInterval(function() {
        if (activeTab === 'stats') fetchEvictionStats();
        if (activeTab === 'peers' && Date.now() - peersLastFetch >= 3000) fetchPeersData();
    }, 5000);

    // Snapshot Sankey data every 12 seconds for rate-based EMA.
    // Stores unfiltered (bucketed) snapshots; filters applied at render time.
    setInterval(function() {
        if (txs.size === 0) return;
        var rawSnap = computeRawSnapshot();
        sankeySnapshots.push(rawSnap);
        if (sankeySnapshots.length > MAX_SNAPSHOTS) sankeySnapshots.shift();
        // Incrementally update EMA if in rate mode (not simple average).
        if (sankeyRateMode && isFinite(sankeyHalfLife)) {
            updateEmaState(rawSnap, sankeyHalfLife);
        }
        if (activeTab === 'stats') {
            statsViewDirty = true;
            scheduleRender();
        }
    }, SNAPSHOT_INTERVAL);

    // Half-life slider wiring.
    var smoothingSlider = document.getElementById('smoothing-slider');
    var smoothingValueLabel = document.getElementById('smoothing-value');
    if (smoothingSlider) {
        smoothingSlider.addEventListener('input', function() {
            var v = parseInt(this.value);
            sankeyHalfLife = sliderToHalfLife(v);
            sankeyRateMode = (v > 0);
            smoothingValueLabel.textContent = formatHalfLife(sankeyHalfLife);
            emaState = null; // invalidate — will recompute on next render
            statsViewDirty = true;
            scheduleRender();
        });
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

    // Normalize reject error strings for grouping. Go-ethereum errors follow
    // fmt.Errorf("%w: values...", baseErr, ...) — the base error before the
    // first colon is the stable category, everything after has variable values.
    function normalizeRejectErr(s) {
        var idx = s.indexOf(':');
        if (idx > 0) return s.substring(0, idx);
        return s;
    }

    var STATUS_ORD = {announced:1, requested:2, received:3, pooled:4, included:5, finalized:6, rejected:7, dropped:8};
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
