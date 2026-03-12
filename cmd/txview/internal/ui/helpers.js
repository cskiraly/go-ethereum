// helpers.js — Shared utility functions used by both app.js and detail.html.
// This file must be loaded before app.js and any inline scripts that use these.

var txviewHelpers = (function() {
    'use strict';

    function parseTS(s) {
        if (!s || s === '0001-01-01T00:00:00Z') return null;
        var d = new Date(s);
        return isNaN(d.getTime()) ? null : d;
    }

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

    function escapeHtml(s) {
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    function buildTxFields(hash, info) {
        var base = parseTS(info.FirstSeen);
        return [
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
    }

    function renderFields(fields, container) {
        var html = '';
        for (var i = 0; i < fields.length; i++) {
            html += '<div class="field"><label>' + escapeHtml(fields[i][0]) +
                    '</label><div class="value">' + escapeHtml(String(fields[i][1])) + '</div></div>';
        }
        container.innerHTML = html;
    }

    return {
        parseTS: parseTS,
        msDelta: msDelta,
        formatTS: formatTS,
        escapeHtml: escapeHtml,
        buildTxFields: buildTxFields,
        renderFields: renderFields
    };
})();
