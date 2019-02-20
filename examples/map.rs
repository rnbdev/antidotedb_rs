extern crate antidotedb;

use antidotedb::crdt::{Counter, LWWReg, ORSet, RRMap};
use antidotedb::crdt::{CounterT, MapT, RegT, SetT};
use antidotedb::crdt::{Object, ObjectReset};
use antidotedb::AntidoteDB;

fn main() {
    let mut db = AntidoteDB::connect("localhost", 8087);
    let txn = db.start_transaction(None);

    let map = RRMap::new("123", "rrmap_test");

    let nestedupdates = [
        (Counter::map_key("counter"), Counter::inc_op(2)),
        (ORSet::map_key("orset"), ORSet::add_op(&[2, 1])),
        (LWWReg::map_key("lwwreg"), LWWReg::set_op(3)),
    ];

    db.mult_update_in_transaction(&[map.reset()], &txn)
        .expect("failed to reset");

    db.mult_update_in_transaction(&[map.update(&nestedupdates, &[])], &txn)
        .expect("failed to update");

    let values = db
        .mult_read_in_transaction(&[map.clone()], &txn)
        .expect("failed to read");

    println!("{:?}", values);

    db.commit_transaction(&txn).expect("failed to commit");
}
