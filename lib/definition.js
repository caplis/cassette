var Model = require('./model');
var cql = require('./cql');
var hoek = require('hoek');

module.exports = function ModelDefinition(table, definition, client) {
    var self = {};

    var schema = hoek.clone(definition);
    delete schema.primary_key;
    delete schema.default_order;

    var primary_key = hoek.clone(definition.primary_key);
    var default_order = definition.default_order;

    /**
     * table getter
     */
    self.table = function () {
        return table;
    };

    /**
     * partition_key getter
     */
    self.partition_key = function () {
        if (Array.isArray(primary_key[0])) {
            return hoek.clone(primary_key[0]);
        }
        return [primary_key[0]];
    };

    /**
     * primary_key getter
     * @param {boolean} [flatten=false] - flatten partition key if array
     */
    self.primary_key = function (flatten) {
        if (Array.isArray(primary_key[0]) && flatten) {
            return primary_key[0].concat(primary_key.slice(1));
        }
        return hoek.clone(primary_key);
    };

    /**
     * default_order getter
     */
    self.default_order = function () {
        return default_order;
    };

    /**
     * schema getter
     */
    self.schema = function () {
        return hoek.clone(schema);
    };

    /**
     * Create a model
     * @param {Object} [data] - optional data to initialize model
     */
    self.create = function create(data) {
        return new Model(self, client, data);
    };

    /**
     * Retrieve a model given a primary key
     * @param {Object} params - key/value pairs representing primary key and lookup values
     * @param {function} cb - Callback with error on failure, model on success
     */
    self.get = function get(params, cb) {
        var pk = self.primary_key();
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
                return cb(new Error('Record not found'));
            }
            cb(null, self.create(res.rows[0]));
        });
    };

    return self;
};
