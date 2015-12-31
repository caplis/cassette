'use strict';

let CounterModel = require('./counter_model');
let TableModel = require('./table_model');
let PageableCollection = require('./pageable_collection');
let cql = require('./cql');
let hoek = require('hoek');
let async = require('async');

const DEFAULT_PAGE = 100;
const MANY_REQ_LIMIT = 50;

let Model = (definition, client, data, is_new) => {
    if (definition.counter_key) {
        return new CounterModel(definition, client, data);
    } else {
        return new TableModel(definition, client, data, is_new);
    }
};

class ModelDefinition {
    constructor(table, definition, client) {
        let schema = hoek.clone(definition);
        delete schema.primary_key;
        delete schema.default_order;
        delete schema.page_key;
        delete schema.counter_key;

        let primary_key = hoek.clone(definition.primary_key);
        let default_order = definition.default_order;
        let page_key = definition.page_key;
        let counter_key = definition.counter_key;

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
         * counter_key getter
         */
        Object.defineProperty(this, 'counter_key', {
            enumerable: true,
            get: function() {
                return counter_key;
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
        return Model(this, this.client, data, true);
    }

    /**
     * Retrieve a single model given a primary key
     * @param {Object} params - Key/value pairs representing a primary key
     * @param {function} [cb] - Optional callback with error on failure, model on success
     */
    one(params, cb) {
        let pk_params = {};
        let pk = this.primary_key;

        if (Array.isArray(pk[0])) {
            pk = pk[0].concat(pk.slice(1));
        }
        for (let i = 0; i < pk.length; i++) {
            if (!params[pk[i]]) {
                let err = new Error('Expected a primary key');
                return (cb ? cb(err) : err);
            }
            pk_params[pk[i]] = params[pk[i]];
        }
        if (typeof cb !== 'function') {
            let m = Model(this, this.client, {});
            Object.keys(params).forEach((k) => {
                m[k] = params[k];
            });
            return m;
        }
        let that = this;
        let q = cql.select({params: pk_params}, this);
        this.client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows) || !res.rows[0]) {
                return cb(new Error('Record not found'));
            }
            cb(null, (Model(that, that.client, res.rows[0])));
        });
    }

    /**
     * Retrieve models given an array of primary keys
     * @param {Object} args
     * @param {Object[]} args.params - Array of key/value pairs each representing a primary key
     * @param {boolean} [args.skip_not_found] - Skip NotFound records, default true
     * @param {function} cb - Callback with error on failure, array of models on success
     */
    many(args, cb) {
        if (!args.params || !Array.isArray(args.params)) {
            return process.nextTick(cb, new Error('Invalid params'));
        }
        if (typeof cb !== 'function') {
            return process.nextTick(cb, new Error('Missing callback function'));
        }
        args.skip_not_found = args.skip_not_found === false ? false : true;
        let that = this;
        let object_to_key = (obj, keys) => {
            let pk = [];
            Object.keys(obj).sort().forEach((k) => {
                if (!Array.isArray(keys)) {
                    pk.push(obj[k]);
                } else if (keys.indexOf(k) !== -1) {
                    pk.push(obj[k]);
                }
            });
            return pk.join('.');
        };
        let original_order = args.params.map(object_to_key);
        let models_map = {};
        async.eachLimit(args.params, MANY_REQ_LIMIT, function (key, async_cb) {
            that.one(key, function (err, model) {
                if (err && (!args.skip_not_found || !err.message.match(/not found/i))) {
                    return async_cb(err);
                }
                if (model) {
                    models_map[object_to_key(model.to_object(), model.primary_key)] = model;
                }
                async_cb();
            });
        }, function (err) {
            if (err) {
                return cb(err);
            }
            let out = [];
            for (let i = 0; i < original_order.length; i++) {
                if (models_map[original_order[i]]) {
                    out.push(models_map[original_order[i]]);
                }
            }
            cb(null, out);
        });
    }

    /**
     * Map/reduce
     */
    map(args, cb) {
        args = args || {};
        if (typeof args.reduce !== 'function') {
            return process.nextTick(cb, new Error('reduce is required and must be a function'));
        }
        if (!args.initial_value) {
            return process.nextTick(cb, new Error('initial_value is required'));
        }
        let that = this;
        let qargs = hoek.clone(args);
        let value = hoek.clone(args.initial_value);
        let q = cql.select(qargs, this);
        this.client.eachRow(q.query, q.params, {prepare: true, autoPage: true}, function(n, row) {
            args.reduce((Model(that, that.client, row)), value, n);
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
        qargs.limit = args.limit || DEFAULT_PAGE;
        let that = this;
        let q = cql.select(qargs, this);
        this.client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows)) {
                res.rows = [];
            }
            let models = res.rows.map((row) => {
                return Model(that, that.client, row);
            });
            cb(null, (new PageableCollection(that, that.client, qargs, models)));
        });
    }
}

module.exports = ModelDefinition;
