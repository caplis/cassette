'use strict';

let moment = require('moment');
let hoek = require('hoek');
let cql = require('./cql');
let Types = require('cassandra-driver').types;

class Model {
    constructor(definition, client) {
        let pk = hoek.clone(definition.primary_key);
        if (Array.isArray(pk[0])) {
            pk = pk[0].concat(pk.slice(1));
        }
        Object.defineProperty(this, 'primary_key', {value: pk});

        Object.defineProperty(this, 'definition', {value: definition});

        Object.defineProperty(this, 'client', {value: client});
    }

    _convert(value) {
        let to_str_types = [Types.TimeUuid, Types.Uuid, Types.Long];
        for (let i = 0; i < to_str_types.length; i++) {
            if (value instanceof to_str_types[i]) {
                return value.toString();
            }
        }
        return value;
    }

    _has_primary_key_values() {
        let pk = this.primary_key;
        for (let i = 0; i < pk.length; i++) {
            if (!this._object[pk[i]]) {
                return false;
            }
        }
        return true;
    }

    delete(cb) {
        if (!this._has_primary_key_values()) {
            return process.nextTick(cb, new Error('Primary key values required for delete'));
        }
        let that = this;
        let params = {};
        this.primary_key.forEach((k) => {
            params[k] = that._object[k];
        });
        let q = cql.delete({params: params}, this.definition);
        this.client.execute(q.query, q.params, {prepare: true}, function (err) {
            if (err) {
                return cb(err);
            }
            that._object = {};
            cb();
        });
    }

    to_object(options) {
        options = options || {};
        let object = hoek.clone(this._object);
        if (Array.isArray(options.exclude) && options.exclude.length > 0) {
            options.exclude.forEach((k) => {
                if (object.hasOwnProperty(k)) {
                    delete object[k];
                }
            });
        }
        if (options.date_objects !== true) {
            Object.keys(object).forEach((k) => {
                if (object[k] instanceof Date) {
                    object[k] = moment(object[k]).unix();
                }
            });
        }
        return object;
    }
}

module.exports = Model;
