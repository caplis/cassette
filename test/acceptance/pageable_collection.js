var async = require('async');
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
var posts = cassette.define({
    keyspace:'post',
    table:'post',
    definition: {
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
    },
    client: client
});

var args = {
    limit: 5,
    params: {
        user_id: '22469097056894976'
    }
};

posts.cursor(args, function (err, collection) {
    if (err) {
        console.log(err);
        process.exit();
    }

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
                if (err) { return cb(err); }
                console.log('length:', collection.length);
                collection.each(function(m, i) {
                    console.log(i + ':', m.post_id, ' - ', m.created_at);
                });
                cb();
            });
        },
        function (err) {
            if (err) {
                console.log(err);
            }
            process.exit();
        }
    );
});
