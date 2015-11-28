"use strict";

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
    user_id: joi.string().regex(/\d+/),
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

let params = {user_id:'22469097056894976'};
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
