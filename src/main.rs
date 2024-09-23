use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

//use serde::{Deserialize, Serialize};

//#[derive(Debug, Serialize, Deserialize)]
//struct DataProducer {
//    timestamp: String,
//}
//
//#[derive(Debug, Serialize, Deserialize)]
//struct DataMeta {
//    producer: DataProducer,
//}
//
//#[derive(Debug, Serialize, Deserialize)]
//struct Data {
//    meta: DataMeta,
//}

async fn produce(data: Vec<String>) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // {"meta":{"producer":{"timestamp":"2021-03-24T15:06:17.321710+00:00"}},"method":"DELETE","session_id":"7c28bcf9-be26-4d0b-931a-3374ab4bb458","status":204,"url":"http://www.youku.com","uuid":"831c6afa-375c-4988-b248-096f9ed101f8"}

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = data
        .iter()
        .enumerate()
        .map(|(i, ele)| async move {
            println!("Sending data: {ele}");

            // The send operation on the topic returns a future, which will be
            // completed once the result or failure from Kafka is received.
            let deliver_result = producer
                .send(
                    FutureRecord::to("web_requests")
                        .payload(ele)
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                )
                .await;

            // This will be executed when the result is received.
            println!("Delivery status for message {} received", i);
            return Some(deliver_result);
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        println!("Future completed. Result: {:?}", future.await);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let mut file = File::open("web_requests-100K.json")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let data = contents
        .split("\n")
        .into_iter()
        .filter_map(|e| {
            if e == "" {
                return None;
            }

            Some(e.to_string())
        })
        .take(30_000)
        .collect::<Vec<String>>();

    produce(data).await;

    Ok(())
}
