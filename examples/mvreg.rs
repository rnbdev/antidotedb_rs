extern crate antidotedb;

use antidotedb::crdt::{MVReg, Object, ObjectReset, RegT};
use antidotedb::AntidoteDB;

use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

fn main() {
    let mut db = AntidoteDB::connect("localhost", 8087);
    let txn = db.start_transaction(None);

    let reg = MVReg::new("123", "mvreg_test");

    let op = reg.set(2);

    db.mult_update_in_transaction(&[reg.reset(), op], &txn)
        .expect("failed to update");;

    let values = db
        .mult_read_in_transaction(&[reg.clone()], &txn)
        .expect("failed to read");

    for v in values {
        let reg_r: Vec<_> = v
            .get_mvreg()
            .get_values()
            .iter()
            .map(|bytes| Cursor::new(bytes).read_u64::<BigEndian>().unwrap() as usize)
            .collect();
        println!("{:?}", reg_r);
    }

    let commit_time = db.commit_transaction(&txn).expect("failed to commit");

    let txn2 = db.start_transaction(Some(&commit_time));

    let values = db
        .mult_read_in_transaction(&[reg.clone()], &txn2)
        .expect("failed to read");

    for v in values {
        let reg_r: Vec<_> = v
            .get_mvreg()
            .get_values()
            .iter()
            .map(|bytes| Cursor::new(bytes).read_u64::<BigEndian>().unwrap() as usize)
            .collect();
        println!("{:?}", reg_r);
    }

    let op = reg.set(23);

    db.mult_update_in_transaction(&[op], &txn2)
        .expect("failed to update");;

    let values = db
        .mult_read_in_transaction(&[reg.clone()], &txn2)
        .expect("failed to read");

    for v in values {
        let reg_r: Vec<_> = v
            .get_mvreg()
            .get_values()
            .iter()
            .map(|bytes| Cursor::new(bytes).read_u64::<BigEndian>().unwrap() as usize)
            .collect();
        println!("{:?}", reg_r);
    }

    println!("aborting");

    db.abort_transaction(&txn2).expect("failed to abort");

    let txn2 = db.start_transaction(Some(&commit_time));

    let values = db
        .mult_read_in_transaction(&[reg.clone()], &txn2)
        .expect("failed to read");;

    for v in values {
        let reg_r: Vec<_> = v
            .get_mvreg()
            .get_values()
            .iter()
            .map(|bytes| Cursor::new(bytes).read_u64::<BigEndian>().unwrap() as usize)
            .collect();
        println!("{:?}", reg_r);
    }

    db.commit_transaction(&txn2).expect("failed to commit");
}
