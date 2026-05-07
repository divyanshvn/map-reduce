use anyhow::Error;
use core::todo;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::available_parallelism;
use std::{collections::VecDeque, sync::atomic::AtomicU32};
use tarpc::context;
use tokio::sync::Semaphore;

use crate::worker::WorkerClient;

pub struct Master {
    n_map: u32,    // CAN BE REMOVED
    n_reduce: u32, // CAN BE REMOVED
    workers: Arc<Vec<WorkerClient>>,
    // running_tasks: Vec<(u32, u8)>, // TODO: will improve later. (worker_num , (1 -> map, 2 -> reduce)).
}

#[derive(Debug)]
pub enum WorkerTask {
    Map(u32, u32, fn(&str, &str) -> Vec<(String, String)>), // (num, n_reduce, fn)
    Reduce(u32, u32, fn(&str, &Vec<String>) -> String),     // (num, n_map, fn)
}

#[tarpc::service]
pub trait MasterRpc {
    async fn give_me_task(worker_id: u32) -> WorkerTask;
    async fn are_you_running() -> bool; // always returns true. Though migh not be needed since
    // in case master isn't running, give_me_task by worker itself won't work.
}

impl MasterRpc for Master {
    async fn give_me_task(self, context: tarpc::context::Context, worker_id: u32) -> WorkerTask {
        todo!()
    }

    async fn are_you_running(self, context: tarpc::context::Context) -> bool {
        todo!()
    }
}

impl Master {
    async fn run(
        &mut self,
        filename: String,
        map_fn: fn(&str, &str) -> Vec<(String, String)>,
        reduce_fn: fn(&str, &Vec<String>) -> String,
        n_reduce: u32,
    ) -> Result<(), Error> {
        // construct the available workers queue
        let mut available_workers = VecDeque::new();
        for i in 0..self.workers.len() {
            available_workers.push_back(i);
        }

        // create the file descriptor
        let fd = File::open(filename)?;
        let reader = BufReader::new(fd);

        let available_workers = Arc::new(Mutex::new(available_workers));
        let semaphore = Arc::new(Semaphore::new(self.workers.len()));
        let completed_map_tasks = Arc::new(AtomicU32::new(0));
        let map_fn = Arc::new(map_fn);

        let mut map_tasks = Vec::new();
        for (i, line) in reader.lines().enumerate() {
            let available_workers_cloned = available_workers.clone();
            let workers_cloned = self.workers.clone();
            let semaphore_cloned = semaphore.clone();
            let map_fn_cc = map_fn.clone();

            let task = tokio::spawn(async move {
                let permit = semaphore_cloned.acquire().await.unwrap();
                let mut worker_queue_locked = available_workers_cloned.lock().unwrap();
                let worker_id = worker_queue_locked.pop_front().unwrap(); // safe to unwrap because
                // its length is being tracked through semaphore
                drop(worker_queue_locked);

                let worker1 = workers_cloned.get(worker_id).unwrap();
                worker1
                    .map(context::current(), *map_fn_cc, i as u32, n_reduce)
                    .await
                    .unwrap();
                // TODO: Handling error to be handled in future. ;-p

                let mut worker_queue_locked = available_workers_cloned.lock().unwrap();
                worker_queue_locked.push_back(worker_id);
                drop(worker_queue_locked);

                drop(permit);
            });
            map_tasks.push(task);
        }
        todo!()
    }
}
