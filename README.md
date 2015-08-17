cassette
========

Data modeling for Cassandra.

### Usage
`
var users = cassette.define({
    keyspace: 'user',
    table: 'user',
    definition: {
        user_id: joi.string().regex(/\d+/),
        name: joi.string().min(3),
        primary_key: ['user_id']
    }
});
var user = users.get({user_id:'123'});
user.name = 'Justin';
user.save(); // update user name

user = users.create({user_id:'124', name: 'Tim'});
user.save(); // insert new user 'Tim'
`
