// txview - Transaction lifecycle viewer
(function() {
    'use strict';

    let ws = null;
    let rpcEndpoint = '';
    let rpcId = 1;
    const txs = new Map();       // hash -> latest event
    const pending = new Map();    // rpc id -> callback
    let selectedHash = null;
    let subId = null;

    const statusDot = document.getElementById('status-dot');
    const statusText = document.getElementById('status-text');
    const txTable = document.getElementById('tx-table');
    const detailPanel = document.getElementById('detail-panel');
    const detailContent = document.getElementById('detail-content');
    const filterInput = document.getElementById('filter-input');
    const filterStatus = document.getElementById('filter-status');

    const counters = {
        total: document.getElementById('cnt-total'),
        announced: document.getElementById('cnt-announced'),
        received: document.getElementById('cnt-received'),
        pooled: document.getElementById('cnt-pooled'),
        included: document.getElementById('cnt-included'),
        finalized: document.getElementById('cnt-finalized'),
        rejected: document.getElementById('cnt-rejected'),
    };

    // Fetch config and connect.
    fetch('/config')
        .then(r => r.json())
        .then(cfg => {
            rpcEndpoint = cfg.rpcEndpoint;
            connect();
        })
        .catch(err => {
            statusText.textContent = 'config error: ' + err;
        });

    filterInput.addEventListener('input', renderTable);
    filterStatus.addEventListener('change', renderTable);

    function connect() {
        console.log('[txview] connecting to', rpcEndpoint);
        ws = new WebSocket(rpcEndpoint);

        ws.onopen = function() {
            console.log('[txview] WebSocket connected');
            statusDot.className = 'status-indicator connected';
            statusText.textContent = 'connected';
            // Subscribe to events.
            rpcCall('txtracker_subscribe', ['events'], function(result, err) {
                if (err) {
                    console.error('[txview] subscribe error:', err);
                    return;
                }
                console.log('[txview] subscribed, id:', result);
                subId = result;
            });
        };

        ws.onclose = function(ev) {
            console.log('[txview] WebSocket closed, code:', ev.code, 'reason:', ev.reason, 'clean:', ev.wasClean);
            statusDot.className = 'status-indicator disconnected';
            statusText.textContent = 'disconnected (code ' + ev.code + ')';
            subId = null;
            setTimeout(connect, 3000);
        };

        ws.onerror = function(ev) {
            console.error('[txview] WebSocket error:', ev);
            ws.close();
        };

        ws.onmessage = function(msg) {
            let data;
            try { data = JSON.parse(msg.data); } catch(e) { return; }

            // Subscription notification.
            if (data.method === 'txtracker_subscription' && data.params) {
                handleEvent(data.params.result);
                return;
            }
            // RPC response.
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

    function handleEvent(ev) {
        if (!ev || !ev.txHash) return;
        const hash = ev.txHash;

        // Decrement old status counter if updating existing entry.
        const old = txs.get(hash);
        if (old) {
            decrementCounter(old.newStatus);
        }

        ev._receivedAt = Date.now();
        txs.set(hash, ev);
        incrementCounter(ev.newStatus);
        counters.total.textContent = txs.size;

        updateTableRow(hash, ev);
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

    function matchesFilter(hash, ev) {
        const text = filterInput.value.toLowerCase();
        const status = filterStatus.value;

        if (status && ev.newStatus !== status) return false;
        if (text) {
            const h = hash.toLowerCase();
            const p = (ev.peer || '').toLowerCase();
            if (!h.includes(text) && !p.includes(text)) return false;
        }
        return true;
    }

    function updateTableRow(hash, ev) {
        let row = document.getElementById('row-' + hash);
        const visible = matchesFilter(hash, ev);

        if (!row) {
            row = document.createElement('tr');
            row.id = 'row-' + hash;
            row.addEventListener('click', function() { selectTx(hash); });
            txTable.prepend(row);
        }

        row.style.display = visible ? '' : 'none';
        if (hash === selectedHash) row.classList.add('selected');

        const shortHash = hash.substring(0, 10) + '...' + hash.substring(hash.length - 6);
        const age = ev._receivedAt ? timeSince(Date.now() - ev._receivedAt) : '-';

        row.innerHTML =
            '<td title="' + hash + '">' + shortHash + '</td>' +
            '<td><span class="badge badge-' + ev.newStatus + '">' + ev.newStatus + '</span></td>' +
            '<td>' + (ev.peer || '-') + '</td>' +
            '<td>' + (ev.blockNum || '-') + '</td>' +
            '<td>' + age + '</td>' +
            '<td>' + (ev.rejectErr || '') + '</td>';
    }

    function renderTable() {
        txs.forEach(function(ev, hash) {
            updateTableRow(hash, ev);
        });
    }

    function selectTx(hash) {
        // Deselect previous.
        if (selectedHash) {
            const prev = document.getElementById('row-' + selectedHash);
            if (prev) prev.classList.remove('selected');
        }
        selectedHash = hash;
        const row = document.getElementById('row-' + hash);
        if (row) row.classList.add('selected');

        detailPanel.classList.add('visible');
        detailContent.innerHTML = '<p>Loading...</p>';

        rpcCall('txtracker_getTx', [hash], function(result, err) {
            if (err) {
                detailContent.innerHTML = '<p>Error: ' + JSON.stringify(err) + '</p>';
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
            ['First Seen', formatTime(info.FirstSeen)],
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
            html += '<div class="field"><label>' + label + '</label><div class="value">' + value + '</div></div>';
        }
        detailContent.innerHTML = html;
    }

    function timeSince(ms) {
        if (!ms || ms < 0) return '0s';
        const secs = Math.floor(ms / 1000);
        if (secs < 60) return secs + 's';
        if (secs < 3600) return Math.floor(secs / 60) + 'm ' + (secs % 60) + 's';
        return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm';
    }

    function formatTime(nanos) {
        if (!nanos) return '-';
        // mclock.AbsTime is nanoseconds since geth process start (monotonic).
        const secs = Math.floor(nanos / 1e9);
        if (secs < 60) return secs + 's uptime';
        if (secs < 3600) return Math.floor(secs / 60) + 'm ' + (secs % 60) + 's uptime';
        return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm uptime';
    }
})();
