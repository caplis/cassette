var cass = require('hapi-cassandra-plugin');
var c = cass.Client({username:process.env.PLAT_CASSANDRA_USERNAME, password:process.env.PLAT_CASSANDRA_PASSWORD, hosts:process.env.PLAT_CASSANDRA_HOSTS.split(',')});
var cassette = require('../index');
var cql = require('../lib/cql');
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
var post_model = cassette.define({keyspace:'post', table:'post', definition: post_def});
var params = {user_id:'22469097056894976', post_id:'22'};
var post = post_model.create(params);
post.save(function (err) {
    console.log(post.to_object());
});

// post_model.get(params, function (err, res) {
//     console.log(err);
//     console.log(res.to_object());
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
