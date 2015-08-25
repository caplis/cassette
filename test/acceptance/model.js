var cass = require('cassandra-driver');
var client = new cass.Client({
    contactPoints: ['localhost'],
    authProvider: new cass.auth.PlainTextAuthProvider(
        process.env.PLAT_CASSANDRA_USERNAME,
        process.env.PLAT_CASSANDRA_PASSWORD
    )
});
var cassette = require('../../index');
var joi = require('joi');
var post_def = {
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
var posts = cassette.define({
    keyspace:'post',
    table:'post',
    definition: post_def,
    client: client
});
var params = {user_id:'22469097056894976', post_id:'22'};
var post = posts.create(params);

console.log('FIRST SAVE >>');
post.save(function (err) {
    if (err) {
        console.log(err);
        process.exit();
    }
    console.log(post.model);

    // updat model
    post.subject = 'test';

    console.log('SECOND SAVE >>');
    post.save(function (err) {
        if (err) {
            console.log(err);
            process.exit();
        }
        console.log(post.model);

        console.log('THIRD SAVE >>');
        post.save(function (err) {
            if (err) {
                console.log(err);
                process.exit();
            }
            console.log(post.model);

            console.log('SYNC >>');
            post.sync(function (err) {
                if (err) {
                    console.log(err);
                    process.exit();
                }
                console.log(post.model);
                process.exit();
            });
        });
    });
});
