var ModelDefinition = require('./definition');
var hoek = require('hoek');
var cassandra = require('hapi-cassandra-plugin');

var default_client = null;

/**
 * Factory method for defining models.
 * @param {Object} args - The arguments needed to define a model
 * @param {string} args.keyspace - The keyspace name
 * @param {string} args.table - The table name
 * @param {Object} args.definition - The table definition
 * @param {string} args.definition.primary_key - The table's primary key
 * @param {string} [args.definition.default_order=ASC] - The default order of the table, 'ASC' or 'DESC'
 * @param {Object} [args.client] - The db client, uses a default client if not passed
 */
module.exports = function define(args) {
    hoek.assert(args, 'missing args');
    hoek.assert(args.table, 'table is required');
    hoek.assert(args.definition, 'definition is required');
    hoek.assert(args.definition.primary_key, 'missing primary_key in definition');
    hoek.assert(!args.definition.default_order || ['ASC','DESC'].indexOf(args.definition.default_order) >= 0, 'invalid default_order, must be ASC or DESC');
    hoek.assert(Array.isArray(args.definition.primary_key), 'primary_key must be an Array');
    hoek.assert(!args.client || args.client && typeof args.client.execute === 'function', 'client requires execute function');
    hoek.assert(!args.client || args.client && typeof args.client.batch === 'function', 'client requires batch function');

    // table = keyspace.table
    var table = [args.keyspace, args.table].join('.');

    // default_order = ASC if not set
    args.definition.default_order = args.definition.default_order || 'ASC';

    // use injected client if passed, otherwise default client
    var client = args.client;
    if (!client) {
        client = default_client;
        if (!client) {
            client = default_client = new cassandra.Client({
                hosts: process.env.PLAT_CASSANDRA_HOSTS.split(','),
                username: process.env.PLAT_CASSANDRA_USERNAME,
                password: process.env.PLAT_CASSANDRA_PASSWORD
            });
        }
    }

    return new ModelDefinition(table, args.definition, client);
};
