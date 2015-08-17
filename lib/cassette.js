var ModelDefinition = require('./definition');
var Model = require('./model');
var hoek = require('hoek');
var cassandra = require('hapi-cassandra-plugin');

var default_client = null;

function get_client(client) {
    if (client && client.hasOwnProperty('execute') && client.hasOwnProperty('batch')) {
        return client;
    }
    client = default_client;
    if (!client) {
        client = default_client = new cassandra.Client({
            hosts: process.env.PLAT_CASSANDRA_HOSTS.split(','),
            username: process.env.PLAT_CASSANDRA_USERNAME,
            password: process.env.PLAT_CASSANDRA_PASSWORD
        });
    }
    return client;
}

/**
 * Factory method for defining models.
 * @param {Object} args - The arguments needed to define a model
 * @param {string} args.keyspace - The keyspace name
 * @param {string} args.table - The table name
 * @param {Object} args.definition - The table definition
 * @param {string} args.definition.primary_key - The table's primary key
 * @param {string} [args.definition.default_order=ASC] - The default order of the table, 'ASC' or 'DESC'
 * @param {Object} [args.client] - The db client, uses a default client if not passed or missing required functions
 */
exports.define = function define(args) {
    args = args || {};

    hoek.assert(args.table, 'table is required');
    hoek.assert(args.definition, 'definition is required');
    hoek.assert(args.definition.primary_key, 'missing primary_key in definition');
    hoek.assert(!args.definition.default_order || ['ASC','DESC'].indexOf(args.definition.default_order) >= 0, 'invalid default_order, must be ASC or DESC');
    hoek.assert(Array.isArray(args.definition.primary_key), 'primary_key must be an Array');

    // table = keyspace.table
    var table = [args.keyspace, args.table].join('.');

    // default_order = ASC if not set
    args.definition.default_order = args.definition.default_order || 'ASC';

    // use injected client if passed, otherwise default client
    var client = get_client(args.client);

    return new ModelDefinition(table, args.definition, client);
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
 */
exports.query = function query(args, cb) {
    args = args || {};

    if (!args.query) {
        return cb(new Error('args.query required'));
    }
    args.params = args.params || [];
    args.options = args.options || {};
    if (!args.options.hasOwnProperty('prepare')) {
        // prepared query if not set
        args.options.prepare = true;
    }
    var client = get_client(args.client);
    client.execute(args.query, args.params, args.options, function (err, res) {
        if (err) {
            return cb(err);
        }
        if (args.definition) {
            // return models if definition passed
            var models = res.rows.map(function(row) {
                return new Model(args.definition, client, row);
            });
            cb(null, models);
        } else {
            // raw result
            cb(null, res.rows);
        }
    });
};
