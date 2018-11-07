extern crate antidotedb;

use antidotedb::crdt::{Operation, LWWREG};
use antidotedb::AntidoteDB;

fn main() {
    let mut db = AntidoteDB::connect("localhost", 8087);
    let txn = db.start_transaction(None);

    let reg = LWWREG::new("123", "hello");

    let op = reg.set(2);

    db.mult_update_in_transaction(&[op], &txn);

    let values = db.mult_read_in_transaction(&[reg.clone(), reg.clone()], &txn);

    println!("{:?}", values);

    let commit_time = db.commit_transaction(&txn).unwrap();

    println!("{:?}", txn);

    println!("{:?}", commit_time);

    let txn2 = db.start_transaction(Some(&commit_time));

    let values = db.mult_read_in_transaction(&[reg.clone(), reg.clone()], &txn2);

    println!("{:?}", values);
    let op = reg.set(23);

    db.mult_update_in_transaction(&[op], &txn2);

    let values = db.mult_read_in_transaction(&[reg.clone(), reg.clone()], &txn2);

    println!("{:?}", values);
    println!("aborting");

    db.abort_transaction(&txn2).expect("failed to abort");

    let txn2 = db.start_transaction(Some(&commit_time));

    let values = db.mult_read_in_transaction(&[reg.clone(), reg.clone()], &txn2);

    println!("{:?}", values);

    db.commit_transaction(&txn2);
}
