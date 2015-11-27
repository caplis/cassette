cassette
========

Data modeling for Cassandra.

### Usage

```
#!javascript
let cassandra = require('cassandra-driver');
let client = new cassandra.Client(/* client details here */);
let Cassette = require('cassette');
let cassette = new Cassette(client);
let users = cassette.define({
    keyspace: 'user',
    table: 'user',
    definition: {
        user_id: joi.string().regex(/\d+/),
        name: joi.string().min(3),
        primary_key: ['user_id']
    }
});

let user1 = null;
users.get({user_id:'123'}, function (err, user) {
    // handle potential err
    user1 = user;
    user1.name = 'Justin';
    user1.save(function (err) { // update name
        // handle potential err
        console.log(user1.model)
    });
});

...

let user2 = users.create({user_id:'124', name: 'Tim'});
user2.save(function (err) { // create user
    // handle potential err
    console.log(user2.model)
});

...

cassette.batch([
    {
      model: user1,
      op: 'delete'
    },
    {
      model: user2,
      op: 'delete'
    }
], function (err) {
    // handle potential err
    console.log('Models deleted!');
});
```
