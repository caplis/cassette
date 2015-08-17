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

    self.table = function () {
        return table;
    };

    self.partition_key = function () {
        if (Array.isArray(primary_key[0])) {
            return hoek.clone(primary_key[0]);
        }
        return [primary_key[0]];
    };

    self.primary_key = function (flatten) {
        if (Array.isArray(primary_key[0]) && flatten) {
            return primary_key[0].concat(primary_key.slice(1));
        }
        return hoek.clone(primary_key);
    };

    self.default_order = function () {
        return default_order;
    };

    self.schema = function () {
        return hoek.clone(schema);
    };

    self.create = function create(data) {
        return new Model(self, client, data);
    };

    self.get = function get(args, cb) {
        var pk = self.primary_key();
        if (Array.isArray(pk[0])) {
            pk = pk[0].concat(pk.slice(1));
        }
        for (var i = 0; i < pk.length; i++) {
            if (!args[pk[i]]) {
                return cb(new Error('Expected a primary key'));
            }
        }
        var q = cql.select({params: hoek.clone(args)}, self);
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

    self.query = function query(args, cb) {
        if (!args.query) {
            return cb(new Error('args.query required'));
        }
        args.params = args.params || [];
        args.opts = args.opts || {};
        if (!args.opts.prepare) {
            args.opts.prepare = true;
        }
        client.execute(args.query, args.params, args.opts, function (err, res) {
            if (err) {
                return cb(err);
            }
            return cb(null, res.rows);
        });
    };

    return self;
};
