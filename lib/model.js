var moment = require('moment');
var hoek = require('hoek');
var joi = require('joi');
var cql = require('./cql');

"use strict";

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

module.exports = function Model(definition, client, data) {
    hoek.assert(definition, 'Missing model definition');
    hoek.assert(client, 'Missing data client');

    data = data || {};

    var self = {};
    var model = {};
    var methods = {};
    var modified = Object.keys(data);
    var schema = definition.schema;
    var primary_key = definition.primary_key;

    if (Array.isArray(primary_key[0])) {
        primary_key = primary_key[0].concat(primary_key.slice(1));
    }

    function has_primary_key_values() {
        var pk = hoek.clone(primary_key);
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

    function validate_model(cb) {
        var result = joi.validate(model, schema);
        if (result.error) {
            return cb(result.error);
        }
        model = result.value;
        cb();
    }

    function get_modified_params() {
        var params = {};
        primary_key.forEach(function (key) {
            params[key] = model[key];
        });
        modified.forEach(function (key) {
            params[key] = model[key];
        });
        return params;
    }

    methods.save = function (args, cb) {
        if (typeof args === 'function') {
            cb = args;
            args = {};
        }
        if (!has_primary_key_values()) {
            return cb(new Error('Primary key values required for save'));
        }
        if (modified.length < 1) {
            cb();
        }
        validate_model(function (err) {
            if (err) {
                return cb(err);
            }
            var qargs = hoek.merge(args, {params: get_modified_params()});
            var q = cql.insert(qargs, definition);
            client.execute(q.query, q.params, {prepare: true}, function (err) {
                if (err) {
                    return cb(err);
                }
                modified = [];
                cb();
            });
        });
    };

    methods.sync = function (cb) {
        if (!has_primary_key_values()) {
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
                if (model.hasOwnProperty(k)) {
                    model[k] = convert(res.rows[0][k]);
                }
            });
            modified = [];
            cb();
        });
    };

    methods.delete = function (cb) {
        if (!has_primary_key_values()) {
            return cb(new Error('Primary key values required for delete'));
        }
        var params = {};
        primary_key.forEach(function (k) {
            params[k] = self[k];
        });
        var q = cql.delete({params: params}, definition);
        client.execute(q.query, q.params, {prepare: true}, function (err) {
            if (err) {
                return cb(err);
            }
            model = {};
            modified = [];
            cb();
        });
    };

    // expose model definition as a read-only property
    Object.defineProperty(self, 'definition', {
        enumerable: true,
        get: function () {
            return definition;
        }
    });

    // expose model as a read-only property
    Object.defineProperty(self, 'model', {
        enumerable: true,
        get: function () {
            return hoek.clone(model);
        }
    });

    // model properties
    Object.keys(schema).forEach(function (key) {
        // check schema for conflicts with model methods
        if (Object.keys(methods).indexOf(key) !== -1) {
            return new Error('Schema field "' + key + '" conflicts with model method!');
        }

        // define getter/setter from schema fields
        Object.defineProperty(self, key, {
            get: function() {
                return model[key];
            },
            set: function(value) {
                if (typeof value === 'function') {
                    throw new TypeError('Model field cannot be set to a function');
                }
                if (model[key] === value) {
                    return;
                }
                model[key] = value;
                modified.push(key);
            }
        });

        if (data.hasOwnProperty(key)) {
            model[key] = convert(data[key]);
        }
    });

    return hoek.merge(self, methods);
};
