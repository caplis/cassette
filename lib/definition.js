'use strict';

let Model = require('./model');
let PageableCollection = require('./pageable_collection');
let cql = require('./cql');
let hoek = require('hoek');
let async = require('async');

const MAX_MANY = 100;
const MAX_PAGE = 100;

class ModelDefinition {
    constructor(table, definition, client) {
        let schema = hoek.clone(definition);
        delete schema.primary_key;
        delete schema.default_order;
        delete schema.page_key;

        let primary_key = hoek.clone(definition.primary_key);
        let default_order = definition.default_order;
        let page_key = definition.page_key;

        /**
         * table getter
         */
        Object.defineProperty(this, 'table', {value: table});

        /**
         * default_order getter
         */
        Object.defineProperty(this, 'default_order', {value: default_order});

        /**
         * client getter
         */
        Object.defineProperty(this, 'client', {value: client});

        /**
         * partition_key getter
         */
        Object.defineProperty(this, 'partition_key', {
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
        Object.defineProperty(this, 'primary_key', {
            enumerable: true,
            get: function () {
                return hoek.clone(primary_key);
            }
        });

        /**
         * page_key getter
         */
        Object.defineProperty(this, 'page_key', {
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
        Object.defineProperty(this, 'schema', {
            enumerable: true,
            get: function () {
                return hoek.clone(schema);
            }
        });
    }

    /**
     * Create a model
     * @param {Object} [data] - optional data to initialize model
     */
    create(data) {
        return new Model(this, this.client, data, true);
    }

    /**
     * Retrieve a single model given a primary key
     * @param {Object} params - Key/value pairs representing a primary key
     * @param {function} cb - Callback with error on failure, model on success
     */
    one(params, cb) {
        let pk = this.primary_key;
        if (Array.isArray(pk[0])) {
            pk = pk[0].concat(pk.slice(1));
        }
        for (let i = 0; i < pk.length; i++) {
            if (!params[pk[i]]) {
                return cb(new Error('Expected a primary key'));
            }
        }
        let that = this;
        let q = cql.select({params: hoek.clone(params)}, this);
        this.client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows) || !res.rows[0]) {
                return cb(new Error('Record not found:', pk));
            }
            cb(null, (new Model(that, that.client, res.rows[0])));
        });
    }

    /**
     * Retrieve models given an array of primary keys
     * @param {Object} args
     * @param {Object[]} args.params - Array of key/value pairs each representing a primary key
     * @param {boolean} [args.skip_not_found=false] - Skip records that are not found
     * @param {function} cb - Callback with error on failure, array of models on success
     */
    many(args, cb) {
        if (!args.params || !Array.isArray(args.params)) {
            return cb(new Error('Invalid params'));
        }
        if (args.params.length > MAX_MANY) {
            return cb(new Error('Number of params exceeds max size'));
        }
        let that = this;
        let models = [];
        async.each(args.params, function (key, async_cb) {
            that.one(key, function (err, model) {
                if (err && (!args.skip_not_found || !err.message.match(/not found/))) {
                    return async_cb(err);
                }
                models.push(model);
                async_cb();
            });
        }, function (err) {
            cb(err, models);
        });
    }

    /**
     * Map/reduce
     */
    map(args, cb) {
        args = args || {};
        if (typeof args.reduce !== 'function') {
            return cb(new Error('reduce is required and must be a function'));
        }
        if (!args.initial_value) {
            return cb(new Error('initial_value is required'));
        }
        let that = this;
        let qargs = hoek.clone(args);
        let value = hoek.clone(args.initial_value);
        let q = cql.select(qargs, this);
        this.client.eachRow(q.query, q.params, {prepare: true, autoPage: true}, function(n, row) {
            args.reduce((new Model(that, that.client, row)), value, n);
        }, function (err) {
            cb(err, value);
        });
    }

    /**
     * Retrieve a pageable collection of models
     * @param {Object} args
     * @param {Object} args.params
     * @param {string|number} [args.next]
     * @param {string|number} [args.previous]
     * @param {string} [args.page_key]
     * @param {number} [args.limit]
     */
    cursor(args, cb) {
        args = args || {};
        let qargs = hoek.clone(args);
        qargs.limit = args.limit || MAX_PAGE;
        let that = this;
        let q = cql.select(qargs, this);
        this.client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows)) {
                res.rows = [];
            }
            let models = res.rows.map(function (row) {
                return new Model(that, that.client, row);
            });
            cb(null, (new PageableCollection(that, that.client, qargs, models)));
        });
    }
}

module.exports = ModelDefinition;
