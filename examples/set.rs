extern crate antidotedb;

use antidotedb::crdt::{ORSet, Object, ObjectReset, SetT};
use antidotedb::AntidoteDB;

use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

fn main() {
    let mut db = AntidoteDB::connect("localhost", 8087);
    let txn = db.start_transaction(None);

    // usage of RWSet is same as ORSet.
    let set = ORSet::new("123", "orset_test");

    db.mult_update_in_transaction(&[set.reset(), set.add(&[1, 2, 3])], &txn)
        .expect("failed to update");;

    let values = db
        .mult_read_in_transaction(&[set.clone()], &txn)
        .expect("failed to read");;

    for v in values {
        let set_r: Vec<_> = v
            .get_set()
            .get_value()
            .iter()
            .map(|bytes| Cursor::new(bytes).read_u64::<BigEndian>().unwrap() as usize)
            .collect();
        println!("{:?}", set_r);
    }

    let commit_time = db.commit_transaction(&txn).expect("failed to commit");

    let txn2 = db.start_transaction(Some(&commit_time));

    let values = db
        .mult_read_in_transaction(&[set.clone(), set.clone()], &txn2)
        .expect("failed to read");

    for v in values {
        let set_r: Vec<_> = v
            .get_set()
            .get_value()
            .iter()
            .map(|bytes| Cursor::new(bytes).read_u64::<BigEndian>().unwrap() as usize)
            .collect();
        println!("{:?}", set_r);
    }

    db.abort_transaction(&txn2).expect("failed to abort");
}
