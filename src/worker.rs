use std::{
    collections::{HashMap, LinkedList},
    fs::{self, File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{Read, Write},
};

use anyhow::{Result, anyhow};
use futures::StreamExt;
use serde::Serialize;
use tarpc::{
    client,
    server::{BaseChannel, Channel},
    transport,
};

use crate::{get_map_split_file, get_reduce_split_file};

pub const DELIMITOR: &str = ";";
#[tarpc::service]
pub trait Worker {
    // TODO: i want to make the declarations here more efficient in terms of argumnets (and even
    // the returned values)
    async fn map(
        map_fn: fn(&str, &str) -> Vec<(String, String)>,
        num: u32,
        n_reduce: u32,
    ) -> Result<(), anyhow::Error>; // returns (key, interim_value)
    async fn reduce(
        reduce_fn: fn(&str, &Vec<String>) -> String,
        num: u32,
        n_map: u32,
    ) -> Result<(), anyhow::Error>; // returns (key,
    // final_value)
    async fn exit() -> u8;
}

#[derive(Clone)]
pub struct TaskWorker {}

pub fn generate_hash(key: &str, num_items: u32) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);

    // can later expand size of num_items
    hasher.finish() % num_items as u64
}

pub fn parse_file(file_name: &str) -> Result<Vec<String>, anyhow::Error> {
    let content = fs::read_to_string(file_name)?;
    let v: Vec<String> = content.lines().map(|t| t.to_string()).collect();

    Ok(v)
}

pub fn fill_file(fd: &mut File, item1: &str, item2: &str) -> Result<(), anyhow::Error> {
    let line = format!("{}{}{}\n", item1, DELIMITOR, item2);
    fd.write(&line.into_bytes())?; // ERROR: whyyyy???(from reduce's output.txt)
    Ok(())
}

impl Worker for TaskWorker {
    async fn map(
        self,
        context: tarpc::context::Context,
        map_fn: fn(&str, &str) -> Vec<(String, String)>,
        num: u32,
        n_reduce: u32,
    ) -> Result<(), anyhow::Error> {
        let file_name = get_map_split_file(num);
        let items = parse_file(&file_name)?;

        let mut output_map: HashMap<String, Vec<String>> = HashMap::new();
        for item in items {
            let key_values = map_fn(&file_name, &item);
            for (key, value) in key_values {
                if output_map.contains_key(&key) {
                    output_map.get_mut(&key).unwrap().push(value);
                } else {
                    output_map.insert(key, vec![value]);
                }
            }
        }

        let mut fd_list = Vec::new();
        for i in 0..n_reduce {
            let fd = File::create(get_reduce_split_file(num, i))?;
            fd_list.push(fd);
        }

        for (k, v) in output_map.iter() {
            let k_hash = generate_hash(k, n_reduce);
            let fd = fd_list.get_mut(k_hash as usize).unwrap(); // safe to unwrap (look into
            // generate_hash for the reason)
            fill_file(fd, k, &serde_json::to_string(v)?)?;
        }

        Ok(())
    }

    async fn reduce(
        self,
        context: tarpc::context::Context,
        reduce_fn: fn(&str, &Vec<String>) -> String,
        num: u32,
        n_map: u32,
    ) -> Result<(), anyhow::Error> {
        let mut key_values: HashMap<String, Vec<String>> = HashMap::new();

        for i in 0..n_map {
            let file_name = get_reduce_split_file(i, num);

            let lines = parse_file(&file_name)?;
            for line in lines {
                let Some((key, values)) = line.split_once(DELIMITOR) else {
                    return Err(anyhow!("invalid intermediate file {}", file_name));
                };
                let mut value_vec: Vec<String> = serde_json::from_str(values)?;

                if key_values.contains_key(key) {
                    let keyval_vec = key_values.get_mut(key).unwrap();
                    keyval_vec.append(&mut value_vec);
                } else {
                    key_values.insert(key.to_string(), value_vec);
                }
            }
        }

        let output_filename = "output.txt";
        let mut out_file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(output_filename)
            .unwrap();
        for (key, values) in key_values.iter() {
            // TODO: see if buffered write will make this efficient
            let final_val = reduce_fn(key, values);
            fill_file(&mut out_file, key, &final_val)?;
        }
        Ok(())
    }

    async fn exit(self, context: tarpc::context::Context) -> u8 {
        todo!()
    }
}

pub async fn spawn_and_return() -> Result<WorkerClient> {
    let (tx, rx) = transport::channel::unbounded();
    let client = WorkerClient::new(client::Config::default(), tx).spawn();

    let server = BaseChannel::with_defaults(rx);
    let worker_server = TaskWorker {};

    tokio::spawn(
        server
            .execute(worker_server.serve())
            .for_each(|future| async {
                tokio::spawn(future);
            }),
    );

    Ok(client)
}
