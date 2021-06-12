# PocketDB

A simple embedded key value database. Currently only support `put` and `get` methods. This is definitely **not production ready**.


### Put

Putting the data will store them in the JSON format to the provided location.

```rust
let mut db = open("mydb").unwrap();

db.put(b"Hello".to_vec(), b"World".to_vec());
db.put(b"Name".to_vec(), b"Aziz".to_vec());
db.put(b"Age".to_vec(), b"25".to_vec());
```

### Get

Continuing on the put example, if you have already data present you can use the `get` method to get the value in the `KeyVal` struct.

```rust
let mut db = open("mydb").unwrap();

let kv = db.get(b"Hello".to_vec()).unwrap();
String::from_utf8(kv.val) // World
```

I don't support deleting the keys as of yet. I need to research how other databases do that. The objective here is to create a learning material which can teach others how to create a key value database with almost production grade.

A database is the heart of the software. Understanding them will make you a better use of them. Most of the projects uses a SQL powered relational database. But the relational databases under the hood uses a key value store to store their mission critical data.
