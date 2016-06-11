'use strict';

let hoek = require('hoek');

let construct_where = (params, cql_params) => {
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
};

let update_counter = (type, args, definition) => {
    let schema = definition.schema
        , query = 'UPDATE ' + definition.table + ' SET '
        , op = type === 'increment' ? '+' : '-'
        , primary_key = {}
        , counter_key = definition.counter_key
        , counter_update = ''
        , params = [];

    Object.keys(args.params).forEach((field) => {
        if (schema[field] && definition.primary_key.indexOf(field) !== -1) {
            primary_key[field] = args.params[field];
        }
    });

    counter_update = [counter_key, "=", counter_key, op, args[counter_key]].join(' ');
    query += counter_update;

    // where
    let where_clause = construct_where(primary_key, params);
    query += where_clause;

    if (process.env.DEBUG) {
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
    let fields = args.fields
        , schema = definition.schema
        , primary_key = definition.primary_key
        , page_key = args.page_key
        , default_order = definition.default_order
        , page_token = args.page_token
        , token_field = ''
        , params = [];

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
    let where_clause = construct_where(args.params, params);
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

    if (process.env.DEBUG) {
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
 * Generate an INSERT statement
 */
exports.insert = function insert(args, definition) {
    let schema = definition.schema
        , query = 'INSERT INTO ' + definition.table
        , fields = []
        , params = []
        , markers = [];

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
    if (args.ttl && typeof args.ttl === 'number') {
        query += ' USING TTL ' + args.ttl;
    }

    if (process.env.DEBUG) {
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
 * Generate an increment statement
 */
exports.increment = function increment(args, definition) {
    return update_counter('increment', args, definition);
};

/**
 * Generate an decrement statement
 */
exports.decrement = function decrement(args, definition) {
    return update_counter('decrement', args, definition);
};

/**
 * Generate a DELETE statement
 */
exports.delete = function del(args, definition) {
    let schema = definition.schema
        , query = 'DELETE FROM ' + definition.table
        , params = [];

    Object.keys(args.params).forEach((field) => {
        if (schema[field]) {
            query += params.length === 0 ? ' WHERE ' : ' AND ';
            query += field + ' = ?';
            params.push(args.params[field]);
        }
    });

    if (process.env.DEBUG) {
        console.info('QUERY:', query);
        console.info('PARAMS:', params);
    }

    return {
        query: query,
        params: params,
        definition: definition
    };
};
