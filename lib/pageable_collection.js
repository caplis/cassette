var Model = require('./model');
var hoek = require('hoek');
var cql = require('./cql');

"use strict";

module.exports = function PageableCollection(definition, client, args, items) {
    var self = {};

    function update_items(q, cb) {
        client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows)) {
                res.rows = [];
            }
            items = res.rows.map(function (m) { return definition.create(m); });
            cb(null, hoek.clone(items));
        });
    }

    /**
     * items getter, read-only.
     */
    Object.defineProperty(self, 'items', {
        enumerable: true,
        get: function () {
            return hoek.clone(items);
        }
    });

    /**
     * Convenience getter, equivalent to PageableCollection.items.length
     */
    Object.defineProperty(self, 'length', {
        get: function () {
            return items.length;
        }
    });

    Object.defineProperty(self, 'first', {
        get: function () {
            return items[0];
        }
    });

    Object.defineProperty(self, 'last', {
        get: function () {
            return items[items.length - 1];
        }
    });

    self.next = function(cb) {
        var qargs = hoek.clone(args);
        var page_key = args.page_key || definition.page_key;
        qargs.next = self.last[page_key];
        qargs.page_key = page_key;
        var q = cql.select(qargs, definition);
        update_items(q, cb);
    };

    self.previous = function (cb) {
        var qargs = hoek.clone(args);
        var page_key = args.page_key || definition.page_key;
        qargs.previous = self.first[page_key];
        qargs.page_key = page_key;
        var q = cql.select(qargs, definition);
        update_items(q, cb);
    };

    return self;
};
