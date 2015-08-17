var moment = require('moment');
var hoek = require('hoek');
var cql = require('./cql');

function convert(value) {
    if (typeof value === 'object' && value !== null && value !== undefined) {
        if (value instanceof Date) {
            return moment(value).unix();
        } else if (typeof value.toString === 'function') {
            return value.toString();
        }
    }
    return value;
}

function has_primary_key_values(pk, model) {
    if (Array.isArray(pk[0])) {
        pk = pk[0].concat(pk.slice(1));
    }
    for (var i = 0; i < pk.length; i++) {
        if (!model[pk[i]]) {
            return false;
        }
    }
    return true;
}

module.exports = function Model(definition, client, data) {
    hoek.assert(definition, 'Missing model definition');
    hoek.assert(client, 'Missing data client');

    data = data || {};

    var self = {};
    var model = {};
    var methods = {};
    var schema = definition.schema();
    var primary_key = definition.primary_key(true);

    methods.save = function (args, cb) {
        if (typeof args === 'function') {
            cb = args;
            args = {};
        }
        if (!has_primary_key_values(primary_key, self)) {
            return cb(new Error('Primary key values required for save'));
        }
        var qargs = hoek.merge(args, {params: self.to_object()});
        var q = cql.insert(qargs, definition);
        client.execute(q.query, q.params, {prepare: true}, function (err) {
            if (err) {
                return cb(err);
            }
            cb();
        });
    };

    methods.sync = function (cb) {
        if (!has_primary_key_values(primary_key, self)) {
            return cb(new Error('Primary key values required for sync'));
        }
        var params = {};
        primary_key.forEach(function (k) {
            params[k] = self[k];
        });
        var q = cql.select({params: params, limit: 1}, definition);
        client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows) || !res.rows[0]) {
                return cb(new Error('Record not found'));
            }
            Object.keys(res.rows[0]).forEach(function (k) {
                self[k] = convert(res.rows[0][k]);
            });
            cb();
        });
    };

    methods.to_object = function () {
        return hoek.clone(model);
    };

    // model properties
    Object.keys(schema).forEach(function (key) {
        // check schema for conflicts with model methods
        if (Object.keys(methods).indexOf(key) !== -1) {
            return new Error('Schema field "' + key + '" conflicts with model method!');
        }

        // define getter/setter from schema fields
        Object.defineProperty(self, key, {
            enumerable: true,
            get: function() {
                return model[key];
            },
            set: function(value) {
                if (typeof value === 'function') {
                    throw new TypeError('Model field cannot be set to a function');
                }
                model[key] = value;
            }
        });

        if (data.hasOwnProperty(key)) {
            model[key] = convert(data[key]);
        }
    });

    return hoek.merge(self, methods);
};
