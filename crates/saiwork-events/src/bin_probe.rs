use tokio::sync::broadcast;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (tx, mut rx) = broadcast::channel::<u64>(4);
    let _ = rx.recv().await; // mark lagged? no, it would block
    drop(rx);
    let (tx2, mut rx2) = broadcast::channel::<u64>(4);
    for i in 0..16u64 { tx2.send(i).unwrap(); }
    match rx2.try_recv() {
        Ok(v) => println!("Ok({v})"),
        Err(e) => println!("Err({e:?})"),
    }
    // fresh subscriber
    let mut fresh = tx2.subscribe();
    match fresh.try_recv() {
        Ok(v) => println!("fresh Ok({v})"),
        Err(e) => println!("fresh Err({e:?})"),
    }
    drop(tx);
}
