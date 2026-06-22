use anyhow::Error;
use core::todo;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::available_parallelism;
use std::{collections::VecDeque, sync::atomic::AtomicU32};
use tarpc::context;
use tokio::sync::Semaphore;

use crate::get_map_split_file;
use crate::worker::WorkerClient;

pub struct Master {
    pub workers: Arc<Vec<WorkerClient>>,
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
    pub async fn run(
        &mut self,
        filename: &str,
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
        println!("started reading the file");
        let reader = BufReader::new(fd);

        let available_workers = Arc::new(Mutex::new(available_workers));
        let semaphore = Arc::new(Semaphore::new(self.workers.len()));
        let map_fn = Arc::new(map_fn);

        let mut map_tasks = Vec::new();
        let mut n_map = 0;
        for (i, line) in reader.lines().map(|l| l.unwrap()).enumerate() {
            // TODO: optimize later (eg: take n lines into a file , instead of just one, or maybe
            // even dynamic)
            let map_file_name = get_map_split_file(i as u32);
            let mut fd = File::create(map_file_name).unwrap();
            fd.write(line.as_bytes())?;

            let available_workers_cloned = available_workers.clone();
            let workers_cloned = self.workers.clone();
            let semaphore_cloned = semaphore.clone();
            let map_fn_cc = map_fn.clone();

            let task = tokio::spawn(async move {
                let permit = semaphore_cloned.acquire().await.unwrap();
                let worker_id = {
                    let mut q = available_workers_cloned.lock().unwrap();
                    q.pop_front().unwrap() // safe to unwrap because the queue's length is tracked through semaphore
                };

                let worker = workers_cloned.get(worker_id).unwrap();
                worker
                    .map(context::current(), *map_fn_cc, i as u32, n_reduce)
                    .await
                    .unwrap()
                    .unwrap();
                // TODO: Handling error to be handled in future. ;-p

                {
                    let mut q = available_workers_cloned.lock().unwrap();
                    q.push_back(worker_id);
                }

                drop(permit);
            });
            map_tasks.push(task);
            n_map += 1;
        }

        for task in map_tasks {
            task.await?;
        }

        // Semaphore should get to the original state by now.

        let mut reduce_tasks = Vec::new();
        for i in 0..n_reduce {
            let available_workers_cloned = available_workers.clone();
            let workers_cloned = self.workers.clone();
            let semaphore_cloned = semaphore.clone();
            let reduce_fn_cc = reduce_fn.clone();

            let task = tokio::spawn(async move {
                let permit = semaphore_cloned.acquire().await.unwrap();
                let worker_id = {
                    let mut q = available_workers_cloned.lock().unwrap(); // ERROR: PoisonError
                    q.pop_front().unwrap() // safe to unwrap because the queue's length is tracked through semaphore
                    // ERROR: unwrap panicked!!!
                };

                let worker = workers_cloned.get(worker_id).unwrap();
                worker
                    .reduce(context::current(), reduce_fn_cc, i, n_map)
                    .await
                    .unwrap()
                    .unwrap();
                // TODO: Handling to be handled later here too.

                {
                    let mut q = available_workers_cloned.lock().unwrap();
                    q.push_back(worker_id);
                }

                drop(permit);
            });

            reduce_tasks.push(task);
        }

        for task in reduce_tasks {
            task.await?;
        }

        Ok(())
    }
}
