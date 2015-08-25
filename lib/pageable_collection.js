var hoek = require('hoek');
var Model = require('./model');
var cql = require('./cql');

"use strict";

module.exports = function PageableCollection(definition, client, args, items) {
    args = args || {};

    var self = {};
    var page_key = args.page_key || definition.page_key;
    var next = args.next || null;
    var prev = args.previous || null;

    function update_items(q, cb) {
        client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows)) {
                res.rows = [];
            }
            items = res.rows.map(function (m) {
                return Model(definition, client, m);
            });
            cb(null, hoek.clone(items));
        });
    }

    /**
     * Number of items in the collection
     */
    Object.defineProperty(self, 'length', {
        get: function () {
            return items.length;
        }
    });

    /**
     * First item in the collection
     */
    Object.defineProperty(self, 'first', {
        get: function () {
            return items[0];
        }
    });

    /**
     * Last item in the collection
     */
    Object.defineProperty(self, 'last', {
        get: function () {
            return items[items.length - 1];
        }
    });

    /**
     * Page to next items in collection
     */
    self.next = function (cb) {
        prev = null;
        var qargs = hoek.clone(args);
        qargs.next = next = self.last[page_key];
        qargs.page_key = page_key;
        update_items(cql.select(qargs, definition), cb);
    };

    /**
     * Page to previous items in collection
     */
    self.previous = function (cb) {
        next = null;
        var qargs = hoek.clone(args);
        qargs.previous = prev = self.first[page_key];
        qargs.page_key = page_key;
        update_items(cql.select(qargs, definition), cb);
    };

    /**
     * Retrieve data from db for current items in collection
     */
    self.sync = function (cb) {
        var qargs = hoek.clone(args);
        if (next) {
            qargs.next = next;
        } else if (prev) {
            qargs.previous = prev;
        }
        qargs.page_key = page_key;
        update_items(cql.select(qargs, definition), cb);
    };

    /**
     * Iterate through collection
     */
    self.each = function (fn) {
        for (var i = 0; i < items.length; i++) {
            fn(items[i], i);
        }
    };

    return self;
};
