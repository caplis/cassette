var ModelDefinition = require('./definition');
var Model = require('./model');
var hoek = require('hoek');

"use strict";

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
    var table = [args.keyspace, args.table].join('.');

    // default_order = ASC if not set
    args.definition.default_order = args.definition.default_order || 'ASC';

    return ModelDefinition(table, args.definition, args.client);
};

/**
 * Expose CQL abstraction
 */
exports.cql = require('./cql');

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
            var models = res.rows.map(function(row) {
                return Model(args.definition, args.client, row);
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
 * @param {Object[]} params
 * @param {Object} params[].model - An object or Model to modify. Must have primary key values.
 * @param {string} params[].op - A write opertation to perform: 'insert'|'update'|'delete'
 */
exports.batch = function batch(params, cb) {
    // TODO expose client.batch functionality
    return cb(new Error('Not implemented'));
};
