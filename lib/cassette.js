'use strict';

let ModelDefinition = require('./definition');
let Model = require('./model');
let cql = require('./cql');
let hoek = require('hoek');
let cassandra = require('cassandra-driver');

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
     * Batch insert, update or delete models
     * @param {Object[]} params
     * @param {Object} params[].model - An object or Model to modify. Must have primary key values.
     * @param {string} params[].op - A write opertation to perform ['save' | 'delete']
     * @param {string} params[].options - ['save' | 'delete'] options
     */
    batch(params, cb) {
        if (!Array.isArray(params) || params.length < 1) {
            return process.nextTick(cb, new Error('Invalid params, expected Array'));
        }

        let queries = [];

        for (let i = 0; i < params.length; i++) {
            // check model
            if (!params[i].model || !(params[i].model instanceof Model)) {
                return process.nextTick(cb, new Error('Invalid model: batch index ' + i));
            }
            // check op
            if (['save','delete'].indexOf(params[i].op) === -1) {
                return process.nextTick(cb, new Error('Invalid op: batch index ' + i));
            }
            // derive query and add to batch
            let map = params[i];
            let data = map.model.validate();
            let qargs = hoek.merge((map.options || {}), {params: data});
            let fn = map.op === 'save' ? 'insert' : 'delete';
            queries.push(cql[fn](qargs, map.model.definition));
        }

        console.log('in batch >>>>', this.client);
        this.client.batch(queries, {prepare: true}, function (err) {
            cb(err);
        });
    }
}

module.exports = Cassette;
