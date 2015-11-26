"use strict";

let moment = require('moment');
let hoek = require('hoek');
let joi = require('joi');
let cql = require('./cql');

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

function validate_model(data, schema) {
    let result = joi.validate(data, schema);
    if (result.error) {
        return new Error('Model validation failed: ' + result.error.message);
    }
    return result.value;
}

class Model {
    constructor(definition, client, data, is_create) {
        let _object = data ? validate_model(data, definition.schema) : {};
        if (_object instanceof Error) {
            throw _object;
        }
        let _modified = is_create ? Object.keys(data) : [];
        let that = this;

        Object.defineProperty(this, '_object', {
            enumerable: false,
            get: function() {
                return _object;
            },
            set: function (value) {
                Object.keys(value).forEach(function (k) {
                    if (definition.schema.hasOwnProperty(k)) {
                        _object[k] = value[k];
                    }
                });
            }
        });

        Object.defineProperty(this, '_modified', {
            enumerable: false,
            get: function() {
                return _modified;
            },
            set: function(value) {
                _modified = [];
                if (Array.isArray(value)) {
                    value.forEach(function (k) {
                        if (definition.schema.hasOwnProperty(k)) {
                            _modified.push(k);
                        }
                    });
                }
            }
        });

        Object.keys(definition.schema).forEach(function (key) {
            // check schema for conflicts with model methods
            if (that.hasOwnProperty(key)) {
                return new Error('Schema field "' + key + '" conflicts with Model attributes');
            }

            // define getter/setter from schema fields
            Object.defineProperty(that, key, {
                enumerable: true,
                get: function() {
                    return that._object[key];
                },
                set: function(value) {
                    if (typeof value === 'function') {
                        throw new TypeError('Model field cannot be set to a function');
                    }
                    if (that._object[key] === value) {
                        return;
                    }
                    that._object[key] = value;
                    that._modified.push(key);
                }
            });

            if (data.hasOwnProperty(key)) {
                that._object[key] = convert(data[key]);
            }
        });

        let pk = definition.primary_key;
        if (Array.isArray(pk[0])) {
            pk = pk[0].concat(pk.slice(1));
        }
        Object.defineProperty(this, 'primary_key', {
            enumerable: false,
            get: function () {
                return pk;
            }
        });

        Object.defineProperty(this, 'definition', {
            enumerable: false,
            get: function () {
                return definition;
            }
        });

        Object.defineProperty(this, 'client', {
            enumerable: false,
            get: function () {
                return client;
            }
        });
    }

    _has_primary_key_values() {
        let pk = hoek.clone(this.definition.primary_key);
        if (Array.isArray(pk[0])) {
            pk = pk[0].concat(pk.slice(1));
        }
        for (let i = 0; i < pk.length; i++) {
            if (!this._object[pk[i]]) {
                return false;
            }
        }
        return true;
    }

    _get_modified_params() {
        let that = this;
        let params = {};
        this.primary_key.forEach(function (key) {
            params[key] = that._object[key];
        });
        this._modified.forEach(function (key) {
            params[key] = that._object[key];
        });
        return params;
    }

    save(args, cb) {
        if (typeof args === 'function') {
            cb = args;
            args = {};
        }
        if (!this._has_primary_key_values()) {
            return cb(new Error('Primary key values required for save'));
        }
        if (this._modified.length < 1) {
            return cb();
        }
        let validated_object = validate_model(this._object, this.definition.schema);
        if (validated_object instanceof Error) {
            return cb(validated_object);
        }
        this._object = validated_object;
        let that = this;
        let qargs = hoek.merge(args, {params: this._get_modified_params()});
        let q = cql.insert(qargs, this.definition);
        this.client.execute(q.query, q.params, {prepare: true}, function (err) {
            if (err) {
                return cb(err);
            }
            that._modified = [];
            cb();
        });
    }

    sync(cb) {
        if (!this._has_primary_key_values()) {
            return cb(new Error('Primary key values required for sync'));
        }
        let that = this;
        let params = {};
        this.primary_key.forEach(function (k) {
            params[k] = that._object[k];
        });
        let q = cql.select({params: params, limit: 1}, this.definition);
        this.client.execute(q.query, q.params, {prepare: true}, function (err, res) {
            if (err) {
                return cb(err);
            }
            if (!res || !Array.isArray(res.rows) || !res.rows[0]) {
                return cb(new Error('Record not found'));
            }
            Object.keys(res.rows[0]).forEach(function (k) {
                if (that._object.hasOwnProperty(k)) {
                    that._object[k] = convert(res.rows[0][k]);
                }
            });
            that._modified = [];
            cb();
        });
    }

    delete(cb) {
        if (!this._has_primary_key_values()) {
            return cb(new Error('Primary key values required for delete'));
        }
        let that = this;
        let params = {};
        this.primary_key.forEach(function (k) {
            params[k] = that._object[k];
        });
        let q = cql.delete({params: params}, this.definition);
        this.client.execute(q.query, q.params, {prepare: true}, function (err) {
            if (err) {
                return cb(err);
            }
            that._object = {};
            that._modified = [];
            cb();
        });
    }

    validate() {
        let validated_object = validate_model(this._object, this.definition.schema);
        if (validated_object instanceof Error) {
            return validated_object;
        }
        return this.to_object();
    }

    to_object() {
        let obj = {};
        let that = this;
        var keys = Object.keys(this._object);
        for (let i = 0; i < keys.length; i++) {
            obj[keys[i]] = that._object[keys[i]];
        }
        return obj;
    }
}

module.exports = Model;
