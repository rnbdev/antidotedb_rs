use super::antidote;

use byteorder::{BigEndian, WriteBytesExt};
use protobuf::RepeatedField;

pub trait Object {
    fn new(key: &str, bucket: &str) -> Self;

    fn get_crdt_type() -> antidote::CRDT_type;

    fn get_object(&self) -> antidote::ApbBoundObject;

    fn new_object(key: &str, bucket: &str) -> antidote::ApbBoundObject {
        let mut object = antidote::ApbBoundObject::new();
        object.set_key(key.as_bytes().to_vec());
        object.set_field_type(Self::get_crdt_type());
        object.set_bucket(bucket.as_bytes().to_vec());
        object
    }

    fn map_key(key: &str) -> antidote::ApbMapKey {
        let mut crdt_key = antidote::ApbMapKey::new();
        crdt_key.set_key(key.as_bytes().to_vec());
        crdt_key.set_field_type(Self::get_crdt_type());
        crdt_key
    }
}

// CRDT traits

pub trait ObjectReset: Object {
    fn reset(&self) -> antidote::ApbUpdateOp {
        let mut update_op = antidote::ApbUpdateOperation::new();
        update_op.set_resetop(antidote::ApbCrdtReset::new());

        let mut update_op_obj = antidote::ApbUpdateOp::new();
        update_op_obj.set_boundobject(self.get_object());
        update_op_obj.set_operation(update_op);

        update_op_obj
    }
}

pub trait RegT: Object {
    fn set_op(value: u64) -> antidote::ApbUpdateOperation {
        let mut set_value = antidote::ApbRegUpdate::new();
        let mut buffer = Vec::<u8>::with_capacity(8);
        buffer.write_u64::<BigEndian>(value).unwrap();
        set_value.set_value(buffer);

        let mut update_op = antidote::ApbUpdateOperation::new();
        update_op.set_regop(set_value);

        update_op
    }

    fn set(&self, value: u64) -> antidote::ApbUpdateOp {
        let mut update_op_obj = antidote::ApbUpdateOp::new();
        update_op_obj.set_boundobject(self.get_object());
        update_op_obj.set_operation(Self::set_op(value));

        update_op_obj
    }
}

pub trait CounterT: Object {
    fn inc_op(val: i64) -> antidote::ApbUpdateOperation {
        // pass negative val to decrement
        let mut inc_op = antidote::ApbCounterUpdate::new();
        inc_op.set_inc(val);

        let mut update_op = antidote::ApbUpdateOperation::new();
        update_op.set_counterop(inc_op);

        update_op
    }

    fn inc(&self, val: i64) -> antidote::ApbUpdateOp {
        let mut update_op_obj = antidote::ApbUpdateOp::new();
        update_op_obj.set_boundobject(self.get_object());
        update_op_obj.set_operation(Self::inc_op(val));

        update_op_obj
    }

    fn inc_one(&self) -> antidote::ApbUpdateOp {
        self.inc(1)
    }

    fn dec_one(&self) -> antidote::ApbUpdateOp {
        self.inc(-1)
    }
}

pub trait SetT: Object {
    fn add_op(values: &[u64]) -> antidote::ApbUpdateOperation {
        let mut add_op = antidote::ApbSetUpdate::new();
        add_op.set_optype(antidote::ApbSetUpdate_SetOpType::ADD);

        let value_bytes: Vec<_> = values
            .iter()
            .map(|&val| {
                let mut buffer = Vec::<u8>::with_capacity(8);
                buffer.write_u64::<BigEndian>(val).unwrap();
                buffer
            })
            .collect();
        add_op.set_adds(RepeatedField::from_slice(&value_bytes));

        let mut update_op = antidote::ApbUpdateOperation::new();
        update_op.set_setop(add_op);

        update_op
    }

    fn add(&self, values: &[u64]) -> antidote::ApbUpdateOp {
        let mut update_op_obj = antidote::ApbUpdateOp::new();
        update_op_obj.set_boundobject(self.get_object());
        update_op_obj.set_operation(Self::add_op(values));

        update_op_obj
    }

    fn rem_op(values: &[u64]) -> antidote::ApbUpdateOperation {
        let mut rem_op = antidote::ApbSetUpdate::new();
        rem_op.set_optype(antidote::ApbSetUpdate_SetOpType::REMOVE);

        let value_bytes: Vec<_> = values
            .iter()
            .map(|&val| {
                let mut buffer = Vec::<u8>::with_capacity(8);
                buffer.write_u64::<BigEndian>(val).unwrap();
                buffer
            })
            .collect();
        rem_op.set_rems(RepeatedField::from_slice(&value_bytes));

        let mut update_op = antidote::ApbUpdateOperation::new();
        update_op.set_setop(rem_op);

        update_op
    }

    fn rem(&self, values: &[u64]) -> antidote::ApbUpdateOp {
        let mut update_op_obj = antidote::ApbUpdateOp::new();
        update_op_obj.set_boundobject(self.get_object());
        update_op_obj.set_operation(Self::rem_op(values));

        update_op_obj
    }
}

pub trait FlagT: Object {
    fn set_op(val: bool) -> antidote::ApbUpdateOperation {
        // pass false to disable
        let mut set_op = antidote::ApbFlagUpdate::new();
        set_op.set_value(val);

        let mut update_op = antidote::ApbUpdateOperation::new();
        update_op.set_flagop(set_op);

        update_op
    }

    fn set(&self, val: bool) -> antidote::ApbUpdateOp {
        let mut update_op_obj = antidote::ApbUpdateOp::new();
        update_op_obj.set_boundobject(self.get_object());
        update_op_obj.set_operation(Self::set_op(val));

        update_op_obj
    }

    fn enable(&self) -> antidote::ApbUpdateOp {
        self.set(true)
    }

    fn disable(&self) -> antidote::ApbUpdateOp {
        self.set(false)
    }
}

pub trait MapT: Object {
    fn key(key: &str, crdt_type: antidote::CRDT_type) -> antidote::ApbMapKey {
        let mut crdt_key = antidote::ApbMapKey::new();
        crdt_key.set_key(key.as_bytes().to_vec());
        crdt_key.set_field_type(crdt_type);
        crdt_key
    }

    fn update_op(
        updates: &[(antidote::ApbMapKey, antidote::ApbUpdateOperation)],
        removes: &[antidote::ApbMapKey],
    ) -> antidote::ApbUpdateOperation {
        let update_ops: Vec<_> = updates
            .iter()
            .map(|(key, op)| {
                let mut nestedop = antidote::ApbMapNestedUpdate::new();
                nestedop.set_key(key.clone());
                nestedop.set_update(op.clone());
                nestedop
            })
            .collect();

        let mut map_update = antidote::ApbMapUpdate::new();
        map_update.set_updates(RepeatedField::from_slice(&update_ops));
        map_update.set_removedKeys(RepeatedField::from_slice(removes));

        let mut update_op = antidote::ApbUpdateOperation::new();
        update_op.set_mapop(map_update);

        update_op
    }

    fn update(
        &self,
        updates: &[(antidote::ApbMapKey, antidote::ApbUpdateOperation)],
        removes: &[antidote::ApbMapKey],
    ) -> antidote::ApbUpdateOp {
        let mut update_op_obj = antidote::ApbUpdateOp::new();
        update_op_obj.set_boundobject(self.get_object());
        update_op_obj.set_operation(Self::update_op(updates, removes));

        update_op_obj
    }
}

// Antidote CRDTs

// Last writer wins register

#[derive(Clone, Debug)]
pub struct LWWReg {
    object: antidote::ApbBoundObject,
}

impl Object for LWWReg {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::LWWREG
    }
}

impl RegT for LWWReg {}

// Multi value register

#[derive(Clone, Debug)]
pub struct MVReg {
    object: antidote::ApbBoundObject,
}

impl Object for MVReg {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::MVREG
    }
}
impl RegT for MVReg {}

impl ObjectReset for MVReg {}

// Counter

#[derive(Clone, Debug)]
pub struct Counter {
    object: antidote::ApbBoundObject,
}

impl Object for Counter {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::COUNTER
    }
}

impl CounterT for Counter {}

// Counter with reset

#[derive(Clone, Debug)]
pub struct FatCounter {
    object: antidote::ApbBoundObject,
}

impl Object for FatCounter {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::FATCOUNTER
    }
}

impl CounterT for FatCounter {}

impl ObjectReset for FatCounter {}

// Add wins set

#[derive(Clone, Debug)]
pub struct ORSet {
    object: antidote::ApbBoundObject,
}

impl Object for ORSet {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::ORSET
    }
}

impl SetT for ORSet {}

impl ObjectReset for ORSet {}

// Remove wins set

#[derive(Clone, Debug)]
pub struct RWSet {
    object: antidote::ApbBoundObject,
}

impl Object for RWSet {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::RWSET
    }
}

impl SetT for RWSet {}

impl ObjectReset for RWSet {}

// Enable wins flag

#[derive(Clone, Debug)]
pub struct FlagEW {
    object: antidote::ApbBoundObject,
}

impl Object for FlagEW {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::FLAG_EW
    }
}

impl FlagT for FlagEW {}

impl ObjectReset for FlagEW {}

// Disable wins flag

#[derive(Clone, Debug)]
pub struct FlagDW {
    object: antidote::ApbBoundObject,
}

impl Object for FlagDW {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::FLAG_DW
    }
}

impl FlagT for FlagDW {}

impl ObjectReset for FlagDW {}

// Grow only map

#[derive(Clone, Debug)]
pub struct GMap {
    object: antidote::ApbBoundObject,
}

impl Object for GMap {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::GMAP
    }
}

impl MapT for GMap {}

// Recursive remove map

#[derive(Clone, Debug)]
pub struct RRMap {
    object: antidote::ApbBoundObject,
}

impl Object for RRMap {
    fn new(key: &str, bucket: &str) -> Self {
        Self {
            object: Self::new_object(key, bucket),
        }
    }

    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }

    fn get_crdt_type() -> antidote::CRDT_type {
        antidote::CRDT_type::RRMAP
    }
}

impl MapT for RRMap {}

impl ObjectReset for RRMap {}
