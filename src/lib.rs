extern crate protobuf;

pub mod crdt;

mod antidote;

use protobuf::{Message, ProtobufEnum, RepeatedField};
use std::io::{Read, Write};
use std::net::TcpStream;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::any::Any;

#[derive(Debug)]
pub struct AntidoteDB {
    socket: TcpStream,
}

#[derive(Debug)]
pub struct Transaction {
    id: Vec<u8>,
}

impl AntidoteDB {
    pub fn connect(hostname: &str, port: u16) -> Self {
        Self::connect_with_string(&format!("{}:{}", hostname, port))
    }

    pub fn connect_with_string(st: &str) -> Self {
        AntidoteDB {
            socket: TcpStream::connect(st).expect("error while creating tcp socket"),
        }
    }

    pub fn start_transaction(&mut self, timestamp: Option<&Vec<u8>>) -> Transaction {
        let mut transaction = antidote::ApbStartTransaction::new();
        if let Some(v) = timestamp {
            transaction.set_timestamp(v.clone());
        }
        transaction.set_properties(Default::default());

        self.send_message(antidote::MessageCode::apbStartTransaction, transaction);

        let (code, message) = self.read_message();
        let message = message
            .downcast_ref::<antidote::ApbStartTransactionResp>()
            .expect("Error to StartTransactionResp");

        assert_eq!(code, antidote::MessageCode::apbStartTransactionResp);
        assert!(message.get_success());

        Transaction {
            id: message.get_transaction_descriptor().to_vec(),
        }
    }

    pub fn mult_read_in_transaction<T>(
        &mut self,
        objects: &[T],
        t: &Transaction,
    ) -> Result<Vec<antidote::ApbReadObjectResp>, ()>
    where
        T: crdt::Object,
    {
        let mut read_objects = antidote::ApbReadObjects::new();
        let apb_objects: Vec<_> = objects.iter().map(|x| x.get_object()).collect();
        read_objects.set_boundobjects(RepeatedField::from_slice(&apb_objects));
        read_objects.set_transaction_descriptor(t.id.clone());

        self.send_message(antidote::MessageCode::apbReadObjects, read_objects);

        let (code, message) = self.read_message();
        let message = message
            .downcast::<antidote::ApbReadObjectsResp>()
            .expect("Error to ApbReadObjectsResp");

        assert_eq!(code, antidote::MessageCode::apbReadObjectsResp);

        if message.get_success() {
            Ok(message.get_objects().to_vec())
        } else {
            Err(())
        }
    }

    pub fn mult_update_in_transaction(
        &mut self,
        operations: &[crdt::Operation],
        t: &Transaction,
    ) -> Result<(), ()> {
        let mut update_objects = antidote::ApbUpdateObjects::new();
        let apb_operations: Vec<_> = operations.iter().map(|x| x.get_operation()).collect();
        update_objects.set_updates(RepeatedField::from_slice(&apb_operations));
        update_objects.set_transaction_descriptor(t.id.clone());

        self.send_message(antidote::MessageCode::apbUpdateObjects, update_objects);

        let (code, message) = self.read_message();
        let message = message
            .downcast::<antidote::ApbOperationResp>()
            .expect("error to downcast to ApbOperationResp");

        assert_eq!(code, antidote::MessageCode::apbOperationResp);
        if message.get_success() {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn commit_transaction(&mut self, t: &Transaction) -> Result<Vec<u8>, ()> {
        let mut commit = antidote::ApbCommitTransaction::new();
        commit.set_transaction_descriptor(t.id.clone());

        self.send_message(antidote::MessageCode::apbCommitTransaction, commit);

        let (_, message) = self.read_message();
        let message = message
            .downcast_ref::<antidote::ApbCommitResp>()
            .expect("error to downcast ApbCommitResp");

        Ok(message.get_commit_time().to_vec())
    }

    pub fn abort_transaction(&mut self, t: &Transaction) -> Result<(), ()> {
        let mut commit = antidote::ApbAbortTransaction::new();
        commit.set_transaction_descriptor(t.id.clone());

        self.send_message(antidote::MessageCode::apbAbortTransaction, commit);

        let (_, message) = self.read_message();
        let message = message
            .downcast_ref::<antidote::ApbOperationResp>()
            .expect("error to downcast ApbOperationResp");

        if message.get_success() {
            Ok(())
        } else {
            Err(())
        }
    }

    fn send_message<T>(&mut self, code: antidote::MessageCode, message: T)
    where
        T: Message,
    {
        let mut buffer = Vec::with_capacity(5);

        buffer
            .write_i32::<BigEndian>(message.compute_size() as i32 + 1)
            .unwrap();

        buffer.write_u8(code as u8).expect("couldn't write");

        buffer.extend(message.write_to_bytes().unwrap().drain(..));

        self.socket.write_all(&buffer).expect("couldn't write all");
    }

    fn read_message(&mut self) -> (antidote::MessageCode, Box<Any>) {
        let msg_leng = self
            .socket
            .read_i32::<BigEndian>()
            .expect("error to read message length");
        let msg_code = antidote::MessageCode::from_i32(
            self.socket.read_u8().expect("error to read message code") as i32,
        )
        .expect("error to convert message code");

        let mut read_buffer = vec![0; msg_leng as usize - 1];
        self.socket
            .read_exact(&mut read_buffer)
            .expect("couldn't read");

        (
            msg_code,
            AntidoteDB::parse_from_bytes(msg_code, &read_buffer).unwrap(),
        )
    }

    fn parse_from_bytes(msgcode: antidote::MessageCode, bytes: &[u8]) -> Option<Box<Any>> {
        match msgcode {
            antidote::MessageCode::apbErrorResp => {
                let mut msg = antidote::ApbErrorResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbGetRegResp => {
                let mut msg = antidote::ApbGetRegResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbCounterUpdate => {
                let mut msg = antidote::ApbCounterUpdate::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbGetCounterResp => {
                let mut msg = antidote::ApbGetCounterResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbOperationResp => {
                let mut msg = antidote::ApbOperationResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbSetUpdate => {
                let mut msg = antidote::ApbSetUpdate::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbGetSetResp => {
                let mut msg = antidote::ApbGetSetResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbStartTransactionResp => {
                let mut msg = antidote::ApbStartTransactionResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbReadObjectResp => {
                let mut msg = antidote::ApbReadObjectResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbReadObjectsResp => {
                let mut msg = antidote::ApbReadObjectsResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbCommitResp => {
                let mut msg = antidote::ApbCommitResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            antidote::MessageCode::apbStaticReadObjectsResp => {
                let mut msg = antidote::ApbStaticReadObjectsResp::new();
                msg.merge_from_bytes(&bytes).expect("Cannot read message!");
                Some(Box::new(msg))
            }
            _ => None,
        }
    }
}
