'use strict';

let async = require('async');
let cass = require('cassandra-driver');
let client = new cass.Client({
    contactPoints: ['localhost'],
    authProvider: new cass.auth.PlainTextAuthProvider(
        'cassandra',
        'cassandra'
    )
});
let Cassette = require('../../index');
let cassette = new Cassette(client);
let joi = require('joi');
let user_def = {
    user_id: joi.string().regex(/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/),
    name: joi.string().min(3),
    created_at: joi.date(),
    updated_at: joi.date(),
    primary_key: ['user_id']
};
let users = cassette.define({
    keyspace:'test',
    table:'user',
    definition: user_def
});

async.series({
    test_map: function (async_cb) {
        let args = {
            params: {user_id: '22469097056894976'},
            initial_value: [],
            reduce: function (m, v) {
                if (m.created_at < 1433797131 && m.created_at > 1433264231) {
                    v.push(m);
                }
            }
        };

        users.map(args, function (err, res) {
            if (err) {
                return async_cb(err);
            }
            console.log('Map Results:');
            res = res.map(function(m){
                return m.created_at;
            });
            console.log(res);
            async_cb();
        });
    },

    test_cursor: function (async_cb) {
        let args = {
            limit: 5,
            params: {user_id: '22469097056894976'}
        };
        users.cursor(args, function (err, collection) {
            if (err) {
                return async_cb(err);
            }
            console.log('Cursor results:');
            console.log('length:', collection.length);
            collection.each(function(m, i) {
                console.log(i + ':', m.post_id, ' - ', m.created_at);
            });

            async.whilst(
                function () {
                    return collection.length > 0;
                },
                function (cb) {
                    collection.next(function (err) {
                        if (err) {
                            return cb(err);
                        }
                        console.log('length:', collection.length);
                        collection.each(function(m, i) {
                            console.log(i + ':', m.post_id, ' - ', m.created_at);
                        });
                        cb();
                    });
                },
                async_cb
            );
        });
    }
}, function (err) {
    if (err) {
        console.log('ERROR:', err);
    }
    process.exit();
});
