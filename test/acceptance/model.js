'use strict';

let Cassette = require('../../index');
let cassandra = require('cassandra-driver');
let client = new cassandra.Client({
    contactPoints: ['localhost'],
    authProvider: new cassandra.auth.PlainTextAuthProvider('cassandra','cassandra')
});
let cassette = new Cassette(client);
let joi = require('joi');
let user_def = {
    user_id: joi.string().regex(/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/),
    name: joi.string().min(3),
    active: joi.boolean().default(true),
    created_at: joi.date(),
    updated_at: joi.date(),
    primary_key: ['user_id']
};
let users = cassette.define({
    keyspace:'test',
    table:'user',
    definition: user_def
});

let params = {user_id:'694feda1-95a0-11e5-aa38-864f75a80d54'};
let user = users.create(params);
console.log(user);
console.log('FIRST SAVE >>');
user.save(function (err) {
    if (err) {
        console.log(err);
        process.exit();
    }
    console.log(user.to_object());

    // update model
    user.name = 'John Smith';

    console.log('SECOND SAVE >>');
    user.save(function (err) {
        if (err) {
            console.log(err);
            process.exit();
        }
        console.log(user.to_object());
        user.name = 'Jane Smith';

        console.log('THIRD SAVE >>');
        user.save(function (err) {
            if (err) {
                console.log(err);
                process.exit();
            }
            console.log(user.to_object());

            console.log('SYNC >>');
            user.sync(function (err) {
                if (err) {
                    console.log(err);
                    process.exit();
                }
                user.name = 'John Q. Voter';
                console.log(user);
                console.log(user.to_object());
                user.delete(function(err) {
                    if (err) {
                        console.log(err);
                        process.exit();
                    }
                    console.log('deleted');
                    process.exit();
                });
            });
        });
    });
});
