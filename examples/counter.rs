extern crate antidotedb;

use antidotedb::crdt::{CounterT, FatCounter, Object, ObjectReset};
use antidotedb::AntidoteDB;

fn main() {
    let mut db = AntidoteDB::connect("localhost", 8087);
    let txn = db.start_transaction(None);

    // usage of Counter is same as FatCounter except Counter does not support reset.
    let counter = FatCounter::new("123", "fatcounter_test");

    // only allowed in fatcounter
    db.mult_update_in_transaction(&[counter.reset()], &txn)
        .expect("failed to update");

    db.mult_update_in_transaction(
        &[
            counter.reset(),
            counter.inc_one(),
            counter.inc_one(),
            counter.inc_one(),
            counter.inc(-5),
        ],
        &txn,
    )
    .expect("failed to update");

    let values = db
        .mult_read_in_transaction(&[counter.clone()], &txn)
        .expect("failed to read");

    for v in values {
        let counter_r = v.get_counter().get_value();
        println!("{:?}", counter_r);
    }

    let commit_time = db.commit_transaction(&txn).expect("failed to commit");

    let txn2 = db.start_transaction(Some(&commit_time));

    let values = db
        .mult_read_in_transaction(&[counter.clone()], &txn2)
        .expect("failed to read");

    for v in values {
        let counter_r = v.get_counter().get_value();
        println!("{:?}", counter_r);
    }

    db.abort_transaction(&txn2).expect("failed to abort");
}
