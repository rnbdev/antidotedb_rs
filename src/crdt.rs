use super::antidote;

use byteorder::{BigEndian, WriteBytesExt};

pub trait Object {
    fn get_object(&self) -> antidote::ApbBoundObject;
}

pub struct Operation {
    object: antidote::ApbUpdateOp,
}

impl Operation {
    pub fn get_operation(&self) -> antidote::ApbUpdateOp {
        self.object.clone()
    }
}

#[derive(Clone, Debug)]
pub struct LWWREG {
    object: antidote::ApbBoundObject,
}

impl Object for LWWREG {
    fn get_object(&self) -> antidote::ApbBoundObject {
        self.object.clone()
    }
}

impl LWWREG {
    pub fn new(key: &str, bucket: &str) -> Self {
        let mut object = antidote::ApbBoundObject::new();
        object.set_key(key.as_bytes().to_vec());
        object.set_field_type(antidote::CRDT_type::LWWREG);
        object.set_bucket(bucket.as_bytes().to_vec());
        LWWREG { object }
    }

    pub fn set(&self, value: u64) -> Operation {
        let mut set_value = antidote::ApbRegUpdate::new();
        let mut buffer = Vec::<u8>::with_capacity(8);
        buffer.write_u64::<BigEndian>(value).unwrap();
        set_value.set_value(buffer);

        let mut update_op = antidote::ApbUpdateOperation::new();
        update_op.set_regop(set_value);

        let mut update_op_obj = antidote::ApbUpdateOp::new();
        update_op_obj.set_boundobject(self.object.clone());
        update_op_obj.set_operation(update_op);

        Operation {
            object: update_op_obj,
        }
    }
}
