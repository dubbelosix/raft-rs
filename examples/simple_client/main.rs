use hdrhistogram::Histogram;
use reqwest;
use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};
use rand::{distributions::Standard, Rng};

const NUM_PAYLOADS: usize = 100000;
const PAYLOAD_SIZE: usize = 512; // Updated to 512 bytes
const RPS: u64 = 100;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let router_endpoint = "http://172.31.35.207:8080/post"; // Updated endpoint
    let rng = rand::thread_rng();

    // Generate random payloads (only values)
    let payloads: Vec<Vec<u8>> = (0..NUM_PAYLOADS)
        .map(|_| rng.clone().sample_iter(Standard).take(PAYLOAD_SIZE).collect())
        .collect();

    let client = reqwest::Client::new();

    // Shared histogram
    let latency_histogram = Arc::new(tokio::sync::Mutex::new(
        Histogram::<u64>::new(4).expect("Failed to create histogram"),
    ));

    // Create a future for each payload
    let futures: Vec<_> = payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let client = client.clone();
            let histogram = latency_histogram.clone();

            tokio::spawn(async move {
                sleep(Duration::from_secs_f64(index as f64 / RPS as f64)).await;
                let start = Instant::now();
                let resp = client.post(router_endpoint).body(payload).send().await;
                match resp {
                    Ok(response) => {
                        if !response.status().is_success() {
                            println!("Failed request with status: {}", response.status());
                        }
                    }
                    Err(e) => println!("Request error: {}", e),
                }
                let elapsed = start.elapsed().as_micros() as u64;

                let mut hist = histogram.lock().await;
                hist.record(elapsed).expect("Failed to record value");
            })
        })
        .collect();

    // Await all futures
    for future in futures {
        future.await?;
    }

    // Print histogram statistics
    let histogram = latency_histogram.lock().await;
    println!("P50 latency: {} us", histogram.value_at_quantile(0.50));
    println!("P90 latency: {} us", histogram.value_at_quantile(0.90));
    println!("P99 latency: {} us", histogram.value_at_quantile(0.99));
    println!("P99.99 latency: {} us", histogram.value_at_quantile(0.9999));

    Ok(())
}
