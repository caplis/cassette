"use strict";

let ModelDefinition = require('./definition');
let Model = require('./model');
let cql = require('./cql');
let hoek = require('hoek');

/**
 * Factory method for defining models.
 * @param {Object} args - The arguments needed to define a model
 * @param {string} args.keyspace - The keyspace name
 * @param {string} args.table - The table name
 * @param {Object} args.definition - The table definition
 * @param {string} args.definition.primary_key - The table's primary key
 * @param {string} [args.definition.default_order=ASC] - The default order of the table, 'ASC' or 'DESC'
 * @param {Object} args.client - The db client
 */
exports.define = function define(args) {
    args = args || {};

    hoek.assert(args.table, 'table is required');
    hoek.assert(args.definition, 'definition is required');
    hoek.assert(args.definition.primary_key, 'missing primary_key in definition');
    hoek.assert(!args.definition.default_order || ['ASC','DESC'].indexOf(args.definition.default_order) >= 0, 'invalid default_order, must be ASC or DESC');
    hoek.assert(Array.isArray(args.definition.primary_key), 'primary_key must be an Array');
    hoek.assert(args.client, 'DB client required');
    hoek.assert(typeof args.client.execute === 'function', 'DB client missing execute function');
    hoek.assert(typeof args.client.batch === 'function', 'DB client missing batch function');

    // table = keyspace.table
    let table = [args.keyspace, args.table].join('.');

    // default_order = ASC if not set
    args.definition.default_order = args.definition.default_order || 'ASC';

    return new ModelDefinition(table, args.definition, args.client);
};

/**
 * Expose CQL abstraction
 */
exports.cql = cql;

/**
 * Query
 * @param {Object} args
 * @param {string} args.query - The query string
 * @param {string[]|number[]} args.params - Array of query params
 * @param {Object} [args.definition] - A model definition, used to return models instead of raw result
 * @param {Object} args.client - A db client
 */
exports.query = function query(args, cb) {
    args = args || {};

    if (!args.query) {
        return cb(new Error('args.query required'));
    }
    if (!args.client || typeof args.client.execute !== 'function') {
        return cb(new Error('args.client required'));
    }
    args.params = args.params || [];
    args.options = args.options || {};
    if (!args.options.hasOwnProperty('prepare')) {
        // prepared query if not set
        args.options.prepare = true;
    }
    args.client.execute(args.query, args.params, args.options, function (err, res) {
        if (err) {
            return cb(err);
        }
        if (args.definition) {
            // return models if definition passed
            let models = res.rows.map(function(row) {
                return new Model(args.definition, args.client, row);
            });
            cb(null, models);
        } else {
            // raw result
            cb(null, res.rows);
        }
    });
};

/**
 * Batch insert, update or delete models
 * @param {Object} args
 * @param {Object[]} args.params
 * @param {Object} args.params[].model - An object or Model to modify. Must have primary key values.
 * @param {string} args.params[].op - A write opertation to perform: 'save'|'delete'
 * @param {string} args.params[].options - save or delete options
 */
exports.batch = function batch(args, cb) {
    args = args || {};

    if (!args.client) {
        return cb(new Error('Missing required arg: client'));
    }

    if (!Array.isArray(args.params) || args.params.length < 1) {
        return cb(new Error('Invalid params, expected Array'));
    }

    let queries = [];

    // check batched model and op
    for (let i = 0; i < args.params.length; i++) {
        if (!args.params[i].model || !(args.params[i].model instanceof Model)) {
            return cb(new Error('Invalid model: batch index ' + i));
        } else if (['save','delete'].indexOf(args.params[i].op) === -1) {
            return cb(new Error('Invalid op: batch index ' + i));
        }
        let map = args.params[i];
        let data = map.model.validate();
        let qargs = hoek.merge((map.options || {}), {params: data});
        let fn = map.op === 'save' ? 'insert' : 'delete';
        queries.push(cql[fn](qargs, map.model.definition));
    }

    args.client.batch(queries, {prepare: true}, function (err) {
        cb(err);
    });
};
