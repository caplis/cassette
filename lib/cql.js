var hoek = require('hoek');

"use strict";

module.exports = {
    /**
     * Generate a SELECT statement
     * @param {Object} args
     * @param {Object} params - select query parameters
     * @param {string[]} [args.fields] - select query fields, defaults to all fields in definition
     * @param {number} [args.limit] - query limit
     * @param {string|number} [args.next] - query paging value (cursor) for next result set
     * @param {string|number} [args.previous] - query paging value (cursor) for previous result set
     * @param {string} [args.page_key] - the field used for paging, defaults to last field in primary key
     * @param {string} [args.order] - the order of the result set, ASC or DESC
     * @param {boolean} [args.filtering=false] - whether or not to allow filtering on the query
     */
    select: function select(args, definition) {
        args = args || {};
        var keys = args.params ? Object.keys(args.params) : []
            , fields = args.fields
            , schema = definition.schema
            , primary_key = definition.primary_key
            , page_key = args.page_key
            , default_order = definition.default_order
            , params = [];

        if (Array.isArray(primary_key[0])) {
            primary_key = primary_key[0].concat(primary_key.slice(1));
        }

        if (!fields) {
            var sorted_fields = Object.keys(schema).sort();
            fields = hoek.clone(primary_key);
            sorted_fields.forEach(function (k) {
                if (primary_key.indexOf(k) === -1) {
                    fields.push(k);
                }
            });
        }

        if (Array.isArray(fields)) {
            fields = fields.join(', ');
        }

        // select from
        var query = 'SELECT ' + fields + ' FROM ' + definition.table;
        var key, op;

        // where
        for (var i = 0; i < keys.length; i++) {
            key = keys[i];
            if (i === 0) {
                query = query + ' WHERE ' + key + ' = ?';
            } else {
                query = query + ' AND ' + key + ' = ?';
            }
            params.push(args.params[key]);
        }

        // paging
        if (args.next && page_key) {
            op = default_order === 'DESC' ? ' < ' : ' > ';
            query += ' AND ' + page_key + op + '?';
            params.push(args.next);
        } else if (args.previous && page_key) {
            op = default_order === 'DESC' ? ' > ' : ' < ';
            query += ' AND ' + page_key + op + '?';
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

        return {
            query: query,
            params: params,
            definition: definition
        };
    },

    /**
     * Generate an INSERT statement
     */
    insert: function insert(args, definition) {
        var schema = definition.schema
            , query = 'INSERT INTO ' + definition.table
            , fields = []
            , params = []
            , markers = [];

        Object.keys(args.params).forEach(function (field) {
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

        return {
            query: query,
            params: params,
            definition: definition
        };
    },

    /**
     * Generate a DELETE statement
     */
    delete: function del(args, definition) {
        var schema = definition.schema
            , query = 'DELETE FROM ' + definition.table
            , params = [];

        Object.keys(args.params).forEach(function (field) {
            if (schema[field]) {
                query += params.length === 0 ? ' WHERE ' : ' AND ';
                query += field + ' = ?';
                params.push(args.params[field]);
            }
        });

        return {
            query: query,
            params: params,
            definition: definition
        };
    }
};
