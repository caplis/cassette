"use strict";

let hoek = require('hoek');
let Model = require('./model');
let cql = require('./cql');

class PageableCollection {
    constructor(definition, client, args, items) {
        this._args = args || {};
        this._items = items;
        this._page_key = args.page_key || definition.page_key;
        this._next = args.next || null;
        this._prev = args.previous || null;
        let that = this;

        Object.defineProperty(this, 'definition', {value: definition});
        Object.defineProperty(this, 'client', {value: client});

        /**
         * Number of items in the collection
         */
        Object.defineProperty(this, 'length', {
            get: function () {
                return that._items.length;
            }
        });

        /**
         * First item in the collection
         */
        Object.defineProperty(this, 'first', {
            get: function () {
                return that._items[0];
            }
        });

        /**
         * Last item in the collection
         */
        Object.defineProperty(this, 'last', {
            get: function () {
                return that._items[that._items.length - 1];
            }
        });
    }

    _update_items(q, cb) {
        let that = this;
        this.client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows)) {
                res.rows = [];
            }
            that._items = res.rows.map(function (m) {
                return new Model(that.definition, that.client, m);
            });
            cb(null, hoek.clone(that._items));
        });
    }

    next(cb) {
        this._prev = null;
        let qargs = hoek.clone(this._args);
        qargs.next = this._next = this.last[this._page_key];
        qargs.page_key = this._page_key;
        this._update_items(cql.select(qargs, this.definition), cb);
    }

    /**
     * Page to previous items in collection
     */
    previous(cb) {
        this._next = null;
        let qargs = hoek.clone(this._args);
        qargs.previous = this._prev = this.first[this._page_key];
        qargs.page_key = this._page_key;
        this._update_items(cql.select(qargs, this.definition), cb);
    }

    at(i) {
        return this._items[i];
    }

    /**
     * Retrieve data from db for current items in collection
     */
    sync(cb) {
        let qargs = hoek.clone(this._args);
        if (this._next) {
            qargs.next = this._next;
        } else if (this._prev) {
            qargs.previous = this._prev;
        }
        qargs.page_key = this._page_key;
        this._update_items(cql.select(qargs, this.definition), cb);
    }

    /**
     * Iterate through collection
     */
    each(fn) {
        let that = this;
        for (let i = 0; i < this._items.length; i++) {
            fn(that._items[i], i);
        }
    }
}

module.exports = PageableCollection;
