var Model = require('./model');
var PageableCollection = require('./pageable_collection');
var cql = require('./cql');
var hoek = require('hoek');
var async = require('async');

"use strict";

const MAX_MANY = 20;
const MAX_PAGE = 100;

module.exports = function ModelDefinition(table, definition, client) {
    var self = {};

    var schema = hoek.clone(definition);
    delete schema.primary_key;
    delete schema.default_order;
    delete schema.page_key;

    var primary_key = hoek.clone(definition.primary_key);
    var default_order = definition.default_order;
    var page_key = definition.page_key;

    /**
     * table getter
     */
    Object.defineProperty(self, 'table', {
        enumerable: true,
        get: function () {
            return table;
        }
    });

    /**
     * partition_key getter
     */
    Object.defineProperty(self, 'partition_key', {
        enumerable: true,
        get: function () {
            if (Array.isArray(primary_key[0])) {
                return hoek.clone(primary_key[0]);
            }
            return [primary_key[0]];
        }
    });

    /**
     * primary_key getter
     * @param {boolean} [flatten=false] - flatten partition key if array
     */
    Object.defineProperty(self, 'primary_key', {
        enumerable: true,
        get: function () {
            return hoek.clone(primary_key);
        }
    });

    /**
     * default_order getter
     */
    Object.defineProperty(self, 'default_order', {
        enumerable: true,
        get: function () {
            return default_order;
        }
    });

    /**
     * page_key getter
     */
    Object.defineProperty(self, 'page_key', {
        enumerable: true,
        get: function() {
            if (page_key) {
                return page_key;
            }
            return (primary_key[primary_key.length - 1] || null);
        }
    });

    /**
     * schema getter
     */
    Object.defineProperty(self, 'schema', {
        enumerable: true,
        get: function () {
            return hoek.clone(schema);
        }
    });

    /**
     * Create a model
     * @param {Object} [data] - optional data to initialize model
     */
    self.create = function create(data) {
        return new Model(self, client, data);
    };

    /**
     * Retrieve a single model given a primary key
     * @param {Object} params - Key/value pairs representing a primary key
     * @param {function} cb - Callback with error on failure, model on success
     */
    self.one = function one(params, cb) {
        var pk = self.primary_key;
        if (Array.isArray(pk[0])) {
            pk = pk[0].concat(pk.slice(1));
        }
        for (var i = 0; i < pk.length; i++) {
            if (!params[pk[i]]) {
                return cb(new Error('Expected a primary key'));
            }
        }
        var q = cql.select({params: hoek.clone(params)}, self);
        client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows) || !res.rows[0]) {
                return cb(new Error('Record not found:', pk));
            }
            cb(null, self.create(res.rows[0]));
        });
    };

    /**
     * Retrieve models given an array of primary keys
     * @param {Object} args
     * @param {Object[]} args.params - Array of key/value pairs each representing a primary key
     * @param {boolean} [args.skip_not_found=false] - Skip records that are not found
     * @param {function} cb - Callback with error on failure, array of models on success
     */
    self.many = function many(args, cb) {
        if (!args.params || !Array.isArray(args.params)) {
            return cb(new Error('Invalid params'));
        }
        if (args.params.length > MAX_MANY) {
            return cb(new Error('Number of params exceeds max size'));
        }
        var models = [];
        async.each(args.params, function (key, async_cb) {
            self.one(key, function (err, model) {
                if (err && (!args.skip_not_found || !err.message.match(/not found/))) {
                    return async_cb(err);
                }
                models.push(model);
                async_cb();
            });
        }, function (err) {
            cb(err, models);
        });
    };

    /**
     * Retrieve a collection of models
     * @param {Object} args
     * @param {Object} args.params
     * @param {string|number} [args.next]
     * @param {string|number} [args.previous]
     * @param {string} [args.page_key]
     * @param {number} [args.limit]
     */
    self.cursor = function cursor(args, cb) {
        var qargs = hoek.clone(args);
        qargs = qargs || {};
        qargs.limit = args.limit || MAX_PAGE;
        var q = cql.select(qargs, self);
        client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows)) {
                res.rows = [];
            }
            var models = res.rows.map(function (row) {
                return self.create(row);
            });
            cb(null, new PageableCollection(self, client, qargs, models));
        });
    };

    self.map = function map(args, cb) {
        // TODO expose client.eachRow functionality
        return cb(new Error('Not implemented'));
    };

    return self;
};
