var cass = require('cassandra-driver');
var client = new cass.Client({
    contactPoints: ['localhost'],
    authProvider: new cass.auth.PlainTextAuthProvider(
        process.env.PLAT_CASSANDRA_USERNAME,
        process.env.PLAT_CASSANDRA_PASSWORD
    )
});
var cassette = require('../index');
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
post.save(function (err) {
    if (err) {
        console.log(err);
        process.exit();
    }
    console.log('after save():', post.model);
    post.subject = 'test';
    post.save(function (err) {
        if (err) {
            console.log(err);
            process.exit();
        }
        console.log('after second save():', post.model);
        post.sync(function (err) {
            if (err) {
                console.log(err);
                process.exit();
            }
            console.log('after sync():', post.model);
            delete params.post_id;
            var args = {limit: 5, params: params};
            posts.cursor(args, function (err, collection) {
                collection.items.forEach(function(m) {
                    console.log(m.post_id, ' - ', m.created_at);
                });
                console.log('length:', collection.length);

                collection.next(function(err) {
                    if (err) {
                        console.log(err);
                        process.exit();
                    }
                    collection.items.forEach(function(m) {
                        console.log(m.post_id, ' - ', m.created_at);
                    });
                    console.log('length:', collection.length);
                    collection.next(function(err) {
                        if (err) {
                            console.log(err);
                            process.exit();
                        }
                        collection.items.forEach(function(m) {
                            console.log(m.post_id, ' - ', m.created_at);
                        });
                        console.log('length:', collection.length);
                        process.exit();
                    });
                });
            });
        });
    });
});

// post_model.get(params, function (err, res) {
//     console.log(err);
//     console.log(res.model);
// });

///////////////////////////////////////////////////////////////////////

// c.execute(q.query, q.params, {prepare: true}, function(err,res) {
//     if (err) {
//         console.log('ERROR:', err);
//         process.exit();
//     }
//     var next = res.rows[res.rows.length-1].post_id.toString();
//     q = cql.select({params: params, limit: 10, next: next}, post_model);
//     console.log('second query:', q);
//     c.execute(q.query, q.params, {prepare: true}, function(err,res) {
//         if (err) {
//             console.log('ERROR:', err);
//             process.exit();
//         }
//         console.log(res);
//         process.exit();
//     });
// });
