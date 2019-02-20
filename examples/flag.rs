extern crate antidotedb;

use antidotedb::crdt::{FlagEW, FlagT, Object};
use antidotedb::AntidoteDB;

fn main() {
    let mut db = AntidoteDB::connect("localhost", 8087);
    let txn = db.start_transaction(None);

    // usage of FlagDW is same as FlagEW.
    let flag = FlagEW::new("123", "flagew_test");

    let values = db
        .mult_read_in_transaction(&[flag.clone()], &txn)
        .expect("failed to read");

    for v in values {
        let flag_r = v.get_flag().get_value();
        println!("{:?}", flag_r);
    }

    let commit_time = db.commit_transaction(&txn).expect("failed to commit");

    let txn2 = db.start_transaction(Some(&commit_time));

    let op = flag.set(true);

    db.mult_update_in_transaction(&[op], &txn2)
        .expect("failed to update");

    let values = db
        .mult_read_in_transaction(&[flag.clone()], &txn2)
        .expect("failed to read");

    for v in values {
        let flag_r = v.get_flag().get_value();
        println!("{:?}", flag_r);
    }

    let op = flag.disable();

    db.mult_update_in_transaction(&[op], &txn2)
        .expect("failed to update");

    let values = db
        .mult_read_in_transaction(&[flag.clone()], &txn2)
        .expect("failed to read");

    for v in values {
        let flag_r = v.get_flag().get_value();
        println!("{:?}", flag_r);
    }

    println!("aborting");
    db.abort_transaction(&txn2).expect("failed to abort");

    let txn2 = db.start_transaction(Some(&commit_time));

    let values = db
        .mult_read_in_transaction(&[flag.clone()], &txn2)
        .expect("failed to read");

    for v in values {
        let flag_r = v.get_flag().get_value();
        println!("{:?}", flag_r);
    }

    db.commit_transaction(&txn2).expect("failed to commit");
}
