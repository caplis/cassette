'use strict';

let ModelDefinition = require('./definition');
let Model = require('./model');
let CounterModel = require('./counter_model');
let TableModel = require('./table_model');
let PageableCollection = require('./pageable_collection');
let cql = require('./cql');
let hoek = require('hoek');
let async = require('async');
let cassandra = require('cassandra-driver');

const DEFAULT_NA_BATCH_LIMIT = 200;

class Cassette {
    constructor(client) {
        hoek.assert(client instanceof cassandra.Client, 'Cassandra client required');
        Object.defineProperty(this, 'client', {value: client});
        this.cql = cql;
        this.cassandra = {
            types: cassandra.types,
            errors: cassandra.errors
        };
    }

    /**
     * ModelDefinition factory.
     * @param {Object} args - The arguments needed to define a model
     * @param {string} args.keyspace - The keyspace name
     * @param {string} args.table - The table name
     * @param {Object} args.definition - The table definition
     * @param {string} args.definition.primary_key - The table's primary key
     * @param {string} [args.definition.default_order=ASC] - The default order of the table, 'ASC' or 'DESC'
     */
    define(args) {
        args = args || {};
        hoek.assert(args.keyspace, 'keyspace is required');
        hoek.assert(args.table, 'table is required');
        hoek.assert(args.definition, 'definition is required');
        hoek.assert(args.definition.primary_key, 'missing primary_key in definition');
        hoek.assert(!args.definition.default_order || ['ASC','DESC'].indexOf(args.definition.default_order) >= 0, 'invalid default_order, must be ASC or DESC');
        hoek.assert(Array.isArray(args.definition.primary_key), 'primary_key must be an Array');

        // table = keyspace.table
        let table = [args.keyspace, args.table].join('.');

        // default_order = ASC if not set
        args.definition.default_order = args.definition.default_order || 'ASC';

        return new ModelDefinition(table, args.definition, this.client);
    }

    /**
     * Raw query - use only when necessary
     * @param {Object} args
     * @param {string} args.query - The query string
     * @param {string[]|number[]} args.params - Array of query params
     * @param {Object} [args.definition] - A model definition, used to return models instead of raw result
     */
    query(args, cb) {
        args = args || {};

        if (!args.query) {
            return process.nextTick(cb, new Error('args.query required'));
        }

        args.params = args.params || [];
        args.options = args.options || {};
        if (!args.options.hasOwnProperty('prepare')) {
            // prepared query if not set
            args.options.prepare = true;
        }
        let that = this;
        this.client.execute(args.query, args.params, args.options, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (args.definition) {
                // return models if definition passed
                let Model = args.definition.counter_key ? CounterModel : TableModel;
                let models = res.rows.map((row) => {
                    return new Model(args.definition, that.client, row);
                });
                cb(null, models);
            } else {
                // raw result
                cb(null, res.rows);
            }
        });
    }

    /**
     * Batch insert, update or delete models. All models must be of the same type (eg: CounterModel, TableModel)
     * @param {Object[]} params
     * @param {Object} params[].model - An object or Model to modify. Must have primary key values.
     * @param {string} params[].op - A write opertation to perform ['save' | 'delete']
     * @param {string} params[].options - ['save' | 'delete'] options
     */
    batch(params, cb) {
        if (!Array.isArray(params) || params.length < 1) {
            return process.nextTick(cb, new Error('Invalid params, expected Array'));
        }
        let options = {prepare: true};
        let queries = [];

        for (let i = 0; i < params.length; i++) {
            let data;
            let qargs;
            let fn;
            if (!this.is_model(params[i].model)) {
                return process.nextTick(cb, new Error('Invalid model: batch index ' + i));
            }
            if (!params[i].model.hasOwnProperty(params[i].op)) {
                return process.nextTick(cb, new Error('Invalid op: batch index ' + i));
            }
            // derive query and add to batch
            if (this.is_counter_model(params[i].model)) {
                data = params[i].model._object;
            } else {
                data = params[i].model._is_new ? params[i].model.validate() : params[i].model._get_modified_params();
            }
            if (data instanceof Error) {
                return process.nextTick(cb, data);
            }
            if (this.is_counter_model(params[i].model)) {
                options.counter = true;
                qargs = hoek.merge((params[i].options || {}), {params: data});
                fn = params[i].op;
            } else {
                qargs = hoek.merge((params[i].options || {}), {params: data});
                fn = params[i].op === 'save' ? 'insert' : 'delete';
            }
            queries.push(cql[fn](qargs, params[i].model.definition));
        }

        this.client.batch(queries, options, cb);
    }

    /**
     * Non-atomic batch.
     * @param {Object[]} params
     * @param {Object}   params[].model - An object or Model to modify. Must have primary key values.
     * @param {string}   params[].op - A write opertation to perform ['save' | 'delete']
     * @param {string}   params[].args - op args
     * @param {Object}   options
     * @param {number}   options.limit - parallel request limit, default 200
     * @param {boolean}  options.skip_errors - skip errors, default true
     */
    na_batch(params, options, cb) {
        if (typeof options === 'function') {
            cb = options;
            options = {};
        }
        // validate params
        if (!Array.isArray(params) || params.length < 1) {
            return process.nextTick(cb, new Error('Invalid params, expected Array'));
        }
        for (let i = 0; i < params.length; i++) {
            if (!this.is_model(params[i].model)) {
                return process.nextTick(cb, new Error('Invalid model: batch index ' + i));
            }
            if (!params[i].model.hasOwnProperty(params[i].op)) {
                return process.nextTick(cb, new Error('Invalid op: batch index ' + i));
            }
        }
        let limit = options.limit || DEFAULT_NA_BATCH_LIMIT;
        let skip_errors = options.skip_errors !== false;
        async.eachLimit(params, limit, (param, each_cb) => {
            let args_arr = [(err) => {
                if (!skip_errors) {
                    return each_cb(err);
                }
                each_cb();
            }];
            if (param.args) {
                args_arr.unshift(param.args);
            }
            param.model[param.op].apply(param.model, args_arr);
        }, (err) => {
            cb(err);
        });
    }

    /**
     * Test if an object is a cassette model
     * @param {Object} model - The object to test
     */
    is_model(model) {
        return model instanceof Model;
    }

    /**
     * Test if an object is a table model
     * @param {Object} model - The object to test
     */
    is_table_model(model) {
        return model instanceof TableModel;
    }

    /**
     * Test if an object is a counter model
     * @param {Object} model - The object to test
     */
    is_counter_model(model) {
        return model instanceof CounterModel;
    }

    /**
     * Test if an object is a cassette model
     * @param {Object} definition - The object to test
     */
    is_definition(definition) {
        return definition instanceof ModelDefinition;
    }

    /**
     * Test if an object is a cassette collection
     * @param {Object} collection - The object to test
     */
    is_collection(collection) {
        return collection instanceof PageableCollection;
    }
}

module.exports = Cassette;
