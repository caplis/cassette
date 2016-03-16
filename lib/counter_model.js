'use strict';

let hoek = require('hoek');
let cql = require('./cql');
let Model = require('./model');

class CounterModel extends Model {
    constructor(definition, client, data) {
        super(definition, client, data);
        let _object = {};
        let that = this;

        if (definition.secondary_keys.length > 0) {
            return new Error('Secondary indexes are not supported on counter tables');
        }

        Object.defineProperty(this, 'counter_key', {value: definition.counter_key});

        Object.defineProperty(this, '_object', {
            enumerable: false,
            get: function() {
                return _object;
            },
            set: function (value) {
                Object.keys(value).forEach((k) => {
                    if (definition.schema.hasOwnProperty(k)) {
                        _object[k] = Model._convert(value[k]);
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
                that._object[key] = Model._convert(data[key]);
            }

            // define getters for schema fields
            Object.defineProperty(that, key, {
                configurable: true,
                enumerable: true,
                get: function() { return that._object[key]; }
            });
        });

    }

    _update_counter(type, amount, cb) {
        if (typeof amount === 'function') {
            cb = amount;
            amount = 1;
        }
        if (!this._has_primary_key_values()) {
            return cb(new Error('Primary key values required for counter update'));
        }
        let qargs = {params: {}};

        qargs[this.counter_key] = amount;
        hoek.merge(qargs, {params: this._object});

        let q = cql[type](qargs, this.definition);
        this.client.execute(q.query, q.params, {prepare: true}, cb);
    }

    increment(amount, cb) {
        this._update_counter('increment', amount, cb);
    }

    decrement(amount, cb) {
        this._update_counter('decrement', amount, cb);
    }
}

module.exports = CounterModel;
