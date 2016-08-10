'use strict';

let hoek = require('hoek');
let DEBUG = ['1', 'true'].indexOf(process.env.DEBUG) !== -1;

let internals = {
    construct_where: (params, cql_params) => {
        let where_clause = '';
        let key;
        let keys = Object.keys(params) || [];
        for (let i = 0; i < keys.length; i++) {
            key = keys[i];
            if (i === 0) {
                where_clause = ' WHERE ' + key + ' = ?';
            } else {
                where_clause += ' AND ' + key + ' = ?';
            }
            cql_params.push(params[key]);
        }
        return where_clause;
    },

    update_counter: (type, args, definition) => {
        let schema = definition.schema;
        let query = 'UPDATE ' + definition.table + ' SET ';
        let op = type === 'increment' ? '+' : '-';
        let primary_key = {};
        let counter_key = definition.counter_key;
        let counter_update = '';
        let params = [];

        Object.keys(args.params).forEach((field) => {
            if (schema[field] && definition.primary_key.indexOf(field) !== -1) {
                primary_key[field] = args.params[field];
            }
        });

        counter_update = [counter_key, "=", counter_key, op, args[counter_key]].join(' ');
        query += counter_update;

        // where
        let where_clause = internals.construct_where(primary_key, params);
        query += where_clause;

        if (DEBUG) {
            console.info('QUERY:', query);
            console.info('PARAMS:', params);
        }

        return {
            query: query,
            params: params,
            definition: definition
        };
    },

    collections: {
        list: {
            add: (field) => {
                return [field, '=', field, '+ ?'].join(' ');
            },
            remove: (field) => {
                return [field, '=', field, '- ?'].join(' ');
            },
            prepend: (field) => {
                return [field, '=', '?', '+', field].join(' ');
            }
        },
        set: {
            add: (field) => {
                return [field, '=', field, '+ ?'].join(' ');
            },
            remove: (field) => {
                return [field, '=', field, '- ?'].join(' ');
            }
        }
    }
};

/**
 * Generate a SELECT statement
 * @param {Object} args
 * @param {Object} [args.params] - select query parameters
 * @param {string[]} [args.fields] - select query fields, defaults to all fields in definition
 * @param {number} [args.limit] - query limit
 * @param {string|number} [args.next] - query paging value (cursor) for next result set
 * @param {string|number} [args.previous] - query paging value (cursor) for previous result set
 * @param {string} [args.page_key] - the field used for paging, defaults to last field in primary key
 * @param {string} [args.order] - the order of the result set, ASC or DESC
 * @param {boolean} [args.filtering=false] - whether or not to allow filtering on the query
 */
exports.select = function select(args, definition) {
    args = args || {};
    args.params = args.params || {};
    let fields = args.fields;
    let schema = definition.schema;
    let primary_key = definition.primary_key;
    let page_key = args.page_key;
    let default_order = definition.default_order;
    let page_token = args.page_token;
    let token_field = '';
    let params = [];

    if (Array.isArray(primary_key[0])) {
        primary_key = primary_key[0].concat(primary_key.slice(1));
    }

    if (!fields) {
        let sorted_fields = Object.keys(schema).sort();
        fields = hoek.clone(primary_key);
        sorted_fields.forEach((k) => {
            if (primary_key.indexOf(k) === -1) {
                fields.push(k);
            }
        });
    }

    if (page_token) {
        token_field = ['token(', definition.partition_key.join(','), ')'].join('');
        fields.push(token_field + ' as ' + page_token);
    }

    if (Array.isArray(fields)) {
        fields = fields.join(', ');
    }

    // select from
    let query = 'SELECT ' + fields + ' FROM ' + definition.table;
    let op;

    // where
    let where_clause = internals.construct_where(args.params, params);
    query += where_clause;

    // paging
    if (args.next && page_key) {
        op = default_order === 'DESC' && !page_token ? ' < ' : ' > ';
        if (page_token) {
            query += ' WHERE ' + token_field;
        } else {
            query += ' AND ' + page_key;
        }
        query += op + '?';
        params.push(args.next);
    } else if (args.previous && page_key) {
        op = default_order === 'DESC' && !page_token ? ' > ' : ' < ';
        if (page_token) {
            query += ' WHERE ' + token_field;
        } else {
            query += ' AND ' + page_key;
        }
        query += op + '?';
        params.push(args.previous);
    }

    // order
    if (args.order && ['ASC', 'DESC'].indexOf(args.order) !== -1) {
        query += ' ORDER BY ' + page_key + ' ' + args.order;
    }

    // limit
    if (args.limit && typeof args.limit === 'number' && args.limit > 0) {
        query += ' LIMIT ' + args.limit;
    }

    // filtering
    if (args.filtering) {
        query += ' ALLOW FILTERING';
    }

    if (DEBUG) {
        console.info('QUERY:', query);
        console.info('PARAMS:', params);
    }

    return {
        query: query,
        params: params,
        definition: definition
    };
};

/**
 * Generate an UPDATE statement
 */
// Example args.params
//     params: {
//         asset_id: '1234',
//         tags: {
//             type: 'list' | 'set',
//             op: 'add' | 'remove' | 'prepend' // list only
//             value: ['test']
//         },
//     }
//
exports.update = function update(args, definition) {
    let schema = definition.schema;
    let query = 'UPDATE ' + definition.table + ' SET';
    let keys = Object.keys(args.params);
    let primary_key = definition.primary_key;
    let fields = [];
    let params = [];
    let where_map = {};

    if (Array.isArray(primary_key[0])) {
        primary_key = primary_key[0].concat(primary_key.slice(1));
    }

    for (let i = 0; i < keys.length; i++) {
        let field = keys[i];
        if (!schema[field]) {
            continue;
        }
        let v = args.params[field];
        if (primary_key.indexOf(field) !== -1) {
            where_map[field] = v;
            continue;
        }
        if (v && v.type && v.op && Array.isArray(v.value)) {
            let fn = hoek.reach(internals, ['collections', v.type, v.op].join('.'));
            if (typeof fn === 'function') {
                fields.push(fn(field));
                params.push(v.value);
            }
        } else {
            fields.push(field + ' = ?');
            params.push(v);
        }
    }

    if (Number.isInteger(args.ttl)) {
        query += ' USING TTL ' + args.ttl;
    }

    query += ' ' + fields.join(', ') + internals.construct_where(where_map, params);

    if (args.if_exists) {
        query += ' IF EXISTS';
    }

    return { query, params, definition };
};

/**
 * Generate an INSERT statement
 */
exports.insert = function insert(args, definition) {
    let schema = definition.schema;
    let query = 'INSERT INTO ' + definition.table;
    let fields = [];
    let params = [];
    let markers = [];

    Object.keys(args.params).forEach((field) => {
        if (schema[field]) {
            fields.push(field);
            params.push(args.params[field]);
            markers.push('?');
        }
    });

    // fields and markers
    query += ' (' + fields.join(', ') + ')';
    query += ' VALUES (' + markers.join(', ') + ')';

    // exists check
    if (args.if_not_exists) {
        query += ' IF NOT EXISTS';
    }

    // time to live
    if (Number.isInteger(args.ttl)) {
        query += ' USING TTL ' + args.ttl;
    }

    if (DEBUG) {
        console.info('QUERY:', query);
        console.info('PARAMS:', params);
    }

    return {
        query: query.trim(),
        params: params,
        definition: definition
    };
};

/**
 * Generate an increment statement
 */
exports.increment = function increment(args, definition) {
    return internals.update_counter('increment', args, definition);
};

/**
 * Generate an decrement statement
 */
exports.decrement = function decrement(args, definition) {
    return internals.update_counter('decrement', args, definition);
};

/**
 * Generate a DELETE statement
 */
exports.delete = function del(args, definition) {
    let schema = definition.schema;
    let query = 'DELETE FROM ' + definition.table;
    let params = [];

    Object.keys(args.params).forEach((field) => {
        if (schema[field]) {
            query += params.length === 0 ? ' WHERE ' : ' AND ';
            query += field + ' = ?';
            params.push(args.params[field]);
        }
    });

    if (DEBUG) {
        console.info('QUERY:', query);
        console.info('PARAMS:', params);
    }

    return {
        query: query,
        params: params,
        definition: definition
    };
};
