'use strict';

let hoek = require('hoek');
let cql = require('./cql');
let Model = require('./model');

class CounterModel extends Model {
    constructor(definition, client, data) {
        super(definition, client, data);
        Object.defineProperty(this, 'counter_key', {value: definition.counter_key});
    };

    _update_counter(type, amount, cb) {
        if (typeof amount === 'function') {
            cb = amount;
            amount = 1;
        }
        if (!this._has_primary_key_values()) {
            return cb(new Error('Primary key values required for increment'));
        }
        let qargs = {};

        qargs[this.counter_key] = amount;
        hoek.merge(qargs, this._object);

        let q = cql[type](qargs, this.definition);
        this.client.execute(q.query, q.params, {prepare: true}, cb);
    }

    increment(amount, cb) {
        this._update_counter('increment', amount, cb);
    };

    decrement(amount, cb) {
        this._update_counter('decrement', amount, cb);
    }
}

module.exports = CounterModel;
