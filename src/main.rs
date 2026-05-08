use std::{collections::HashMap, sync::Arc};

use crate::master::Master;

mod master;
mod worker;

pub fn get_reduce_split_file(map_num: u32, reduce_num: u32) -> String {
    format!("reduce_files/Rfile:m{}-r{}", map_num, reduce_num)
}

pub fn get_map_split_file(map_num: u32) -> String {
    format!("map_files/Mfile_{}", map_num)
}

pub static NUM_WORKERS: u32 = 3;
pub static N_REDUCE: u32 = 5;

fn wc_map_fn(filename: &str, text: &str) -> Vec<(String, String)> {
    let mut key_value_map = HashMap::new();
    for word in text.split_whitespace() {
        if key_value_map.contains_key(word) {
            let value = key_value_map.get_mut(word).unwrap();
            *value += 1;
            continue;
        }
        key_value_map.insert(word, 1);
    }

    let mut key_value_vec = Vec::new();
    for (key, value) in key_value_map.iter() {
        key_value_vec.push((key.to_string(), value.to_string()));
    }
    key_value_vec
}

fn wc_reduce_fn(key: &str, values: &Vec<String>) -> String {
    let mut total_val = 0;
    for value in values {
        let val: u32 = value.parse().unwrap();
        total_val += val;
    }

    total_val.to_string()
}

#[tokio::main]
async fn main() {
    let mut worker_clients = Vec::new();
    for i in 0..NUM_WORKERS {
        let worker_client = worker::spawn_and_return().await.unwrap();
        worker_clients.push(worker_client);
    }

    let mut master = Master {
        workers: Arc::new(worker_clients),
    };

    master
        .run("input.txt", wc_map_fn, wc_reduce_fn, N_REDUCE)
        .await
        .unwrap();
}
