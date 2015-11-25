"use strict";

let cass = require('cassandra-driver');
let client = new cass.Client({
    contactPoints: ['localhost'],
    authProvider: new cass.auth.PlainTextAuthProvider(
        process.env.PLAT_CASSANDRA_USERNAME,
        process.env.PLAT_CASSANDRA_PASSWORD
    )
});
let cassette = require('../../index');
let joi = require('joi');
let post_def = {
    user_id: joi.string(),
    post_id: joi.string(),
    subject: joi.string(),
    body: joi.string().allow(''),
    image_id: joi.string(),
    repost_user_id: joi.string(),
    repost_id: joi.string(),
    repost_body: joi.boolean(),
    product_user_id: joi.string(),
    product_id: joi.string(),
    product_currency: joi.string(),
    product_price: joi.number(),
    product_quantity: joi.number(),
    product_link: joi.string().allow(''),
    product_status: joi.string(),
    created_at: joi.date(),
    updated_at: joi.date(),
    primary_key: ['user_id','post_id'],
    default_order: 'DESC'
};
let posts = cassette.define({
    keyspace:'post',
    table:'post',
    definition: post_def,
    client: client
});

let params = {user_id:'22469097056894976', post_id:'22'};
let post = posts.create(params);
console.log(post);
console.log('FIRST SAVE >>');
post.save(function (err) {
    if (err) {
        console.log(err);
        process.exit();
    }
    console.log(post.to_object());

    // updat model
    post.subject = 'test';
    post.body = 'rock you like a hurricane';

    console.log('SECOND SAVE >>');
    post.save(function (err) {
        if (err) {
            console.log(err);
            process.exit();
        }
        console.log(post.to_object());

        console.log('THIRD SAVE >>');
        post.save(function (err) {
            if (err) {
                console.log(err);
                process.exit();
            }
            console.log(post.to_object());

            console.log('SYNC >>');
            post.sync(function (err) {
                if (err) {
                    console.log(err);
                    process.exit();
                }
                post.body = 'this is a test';
                console.log(post);
                console.log(post.to_object());
                // post.delete(function(err) {
                //     if (err) {
                //         console.log(err);
                //         process.exit();
                //     }
                //     console.log('deleted');
                    process.exit();
                // });
            });
        });
    });
});
