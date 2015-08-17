cassette
========

Data modeling for Cassandra.

### Usage

```
#!javascript
var users = cassette.define({
    keyspace: 'user',
    table: 'user',
    definition: {
        user_id: joi.string().regex(/\d+/),
        name: joi.string().min(3),
        primary_key: ['user_id']
    }
});

users.get({user_id:'123'}, function (err, user) {
    // handle err
    user.name = 'Justin';
    user.save(function (err) { // update name
        // handle err
        console.log(user.to_object)
    });
});

...

var user = users.create({user_id:'124', name: 'Tim'});
user.save(function (err) { // create user
    // handle err
    console.log(user.object())
});
```
