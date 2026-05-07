mod master;
mod worker;

pub fn get_reduce_split_file(map_num: u32, reduce_num: u32) -> String {
    format!("Rfile:m{}-r{}", map_num, reduce_num)
}

pub fn get_map_split_file(map_num: u32) -> String {
    format!("Mfile_{}", map_num)
}

fn main() {
    println!("Hello, world!");
}
