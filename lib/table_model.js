'use strict';

let hoek = require('hoek');
let joi = require('joi');
let cql = require('./cql');
let Model = require('./model');

class TableModel extends Model {
    constructor(definition, client, data, is_new) {
        super(definition, client, data, is_new);
        let _object = {};
        let _modified = [];
        let _is_new = is_new;
        let that = this;

        Object.defineProperty(this, '_modified', {
            enumerable: false,
            get: function() {
                return _modified;
            },
            set: function(value) {
                _modified = [];
                if (Array.isArray(value)) {
                    value.forEach((k) => {
                        if (definition.schema.hasOwnProperty(k)) {
                            _modified.push(k);
                        }
                    });
                }
            }
        });

        Object.defineProperty(this, '_is_new', {
            get: function () {
                return _is_new;
            },
            set: function (v) {
                _is_new = !!v;
            }
        });

        Object.defineProperty(this, '_object', {
            enumerable: false,
            get: function() {
                return _object;
            },
            set: function (value) {
                Object.keys(value).forEach((k) => {
                    if (definition.schema.hasOwnProperty(k)) {
                        _object[k] = that._convert(value[k]);
                        if (that._is_new) {
                            that._modified.push(k);
                        }
                    }
                });
            }
        });

        Object.keys(definition.schema).forEach((key) => {
            // check schema for conflicts with model methods
            if (that.hasOwnProperty(key)) {
                return new Error('Schema field "' + key + '" conflicts with internal Model attributes');
            }

            // initial model state
            if (data.hasOwnProperty(key)) {
                that._object[key] = that._convert(data[key]);
                if (that._is_new) {
                    that._modified.push(key);
                }
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
                    value = that._convert(value);
                    if (that._object[key] === value) {
                        return;
                    }
                    that._object[key] = value;
                    that._modified.push(key);
                }
            });
        });
    }

    _get_modified_params() {
        let that = this;
        let params = {};
        this.primary_key.forEach((key) => {
            params[key] = that._object[key];
        });
        this._modified.forEach((key) => {
            params[key] = that._object[key];
        });
        return params;
    }

    validate(cb) {
        let options = {stripUnknown: true};
        let model;
        let schema;
        if (this._is_new) {
            model = this._object;
            schema = this.definition.schema;
        } else {
            options.noDefaults = true;
            model = this._get_modified_params();
            schema = {};
            Object.keys(model).forEach((key) => {
                schema[key] = this.definition.schema[key];
            });
        }
        let that = this;

        if (typeof cb !== 'function') {
            let validated_model = joi.validate(model, schema, options);
            if (validated_model.error) {
                return validated_model.error;
            }
            validated_model = validated_model.value;
            Object.keys(validated_model).forEach((key) => {
                if (that._is_new && !that._object.hasOwnProperty(key)) {
                    that._modified.push(key);
                }
                that._object[key] = that._convert(validated_model[key]);
            });
            return hoek.clone(that._object);
        }

        joi.validate(model, schema, options, function (err, validated_model) {
            if (err) {
                return cb(err);
            }
            Object.keys(validated_model).forEach((key) => {
                if (that._is_new && !that._object.hasOwnProperty(key)) {
                    that._modified.push(key);
                }
                that._object[key] = that._convert(validated_model[key]);
            });
            cb();
        });
    }

    save(args, cb) {
        if (typeof args === 'function') {
            cb = args;
            args = {};
        }
        if (this._modified.length < 1) {
            return process.nextTick(cb);
        }
        let that = this;
        this.validate(function (err) {
            if (err) {
                return cb(err);
            }
            if (!that._has_primary_key_values()) {
                return cb(new Error('Primary key values required for save'));
            }
            let qargs = hoek.merge(args, {params: that._get_modified_params()});
            let q = cql.insert(qargs, that.definition);
            that.client.execute(q.query, q.params, {prepare: true}, function (err, res) {
                if (err) {
                    return cb(err);
                }

                if (args.if_not_exists) {
                    if (!res || !Array.isArray(res.rows) || res.rows.length < 1) {
                        return cb(new Error('Malformed cassandra response'));
                    }
                    if (!res.rows[0]['[applied]']) {
                        return cb(new Error('Record exists'));
                    }
                }

                that._modified = [];
                that._is_new = false;
                cb();
            });
        });
    }

    sync(cb) {
        if (!this._has_primary_key_values()) {
            return process.nextTick(cb, new Error('Primary key values required for sync'));
        }
        let that = this;
        let params = {};
        this.primary_key.forEach((k) => {
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
            Object.keys(res.rows[0]).forEach((k) => {
                that._object[k] = that._convert(res.rows[0][k]);
            });
            that._modified = [];
            cb();
        });
    }

    // override necessary as it clears _modified and _is_new
    delete(cb) {
        let that = this;
        super.delete((err) => {
            if (err) {
                return cb(err);
            }
            that._modified = [];
            that._is_new = true;
            cb();
        });
    }
}

module.exports = TableModel;
