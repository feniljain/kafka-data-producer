use std::error::Error;
// use std::fs::File;
// use std::io::Read;
use std::time::{Duration, Instant};

use chrono::{Days, Utc};
use futures::future::join_all;
use rand::{distributions::Alphanumeric, Rng};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{
    BaseRecord, DefaultProducerContext, FutureProducer, FutureRecord, ThreadedProducer,
};
use rdkafka::util::get_rdkafka_version;
use serde::Serialize;
// use serde_json::{json, Value};
use tokio::time;

#[derive(Serialize)]
struct LogMessage {
    updated_on: String,
    cluster_name: String,
    query_id: String,
    catalog: String,
    query: String,
    end_time: i64,
    is_cached: bool,
    explain_analyse_output: String,
    execution_time: f64,
    workspace_id: String,
    query_hash: String,
    start_time: i64,
    database: String,
    client_perceived_time: f64,
    parsing_time: f64,
    cluster_uuid: String,
    queueing_time: f64,
    alias: String,
    added_on: String,
    email: String,
    status: String,
}

impl LogMessage {
    fn new() -> Self {
        let queries: Vec<String> = vec![
			String::from("SELECT * FROM \"glue\".\"tpcds_1000\".\"call_center\" LIMIT 10"),
			String::from("SELECT count(*) FROM \"glue\".\"tpcds_1000\".\"custome\""),
			String::from("SELECT item_i_id, item_desc FROM \"glue\".\"tpcds_1000\".\"item\" WHERE item_i_id < 100"),
			String::from("SELECT * FROM \"glue\".\"tpcds_1000\".\"store_returns\" ORDER BY sr_returned_date LIMIT 5"),
		];

        let now = Utc::now();

        let mut query_id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        query_id += &now.to_string();

        let query_hash: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        let end_ts = Utc::now().timestamp();
        let start_ts = Utc::now()
            .checked_sub_days(Days::new(1))
            .expect("could not subtract day")
            .timestamp();

        let exec_time = rand::thread_rng().gen::<f64>() * 1000.0;
        let client_perceived_time = rand::thread_rng().gen::<f64>() * 1000.0;
        let parsing_time = rand::thread_rng().gen::<f64>() * 1000.0;
        let queueing_time = rand::thread_rng().gen::<f64>() * 1000.0;

        Self {
            updated_on: now.clone().to_string(),
            cluster_name: String::from("qhv3_1"),
            query_id: query_id,
            catalog: String::from("glue"),
            query: (queries[rand::thread_rng().gen::<usize>() % 4]).clone(),
            end_time: end_ts,
            is_cached: rand::random(),
            explain_analyse_output: String::from("{}"),
            execution_time: exec_time,
            workspace_id: String::from("178"),
            query_hash: query_hash,
            start_time: start_ts,
            database: String::from("tpcds_1000"),
            client_perceived_time: client_perceived_time,
            parsing_time: parsing_time,
            cluster_uuid: String::from("ne433ovcp"),
            queueing_time: queueing_time,
            alias: String::from("cops-beta"),
            added_on: now.clone().to_string(),
            email: String::from("experiments@e6x.io"),
            status: String::from("success"),
        }
    }
}

async fn threaded_producer(data: Vec<String>, producer: &ThreadedProducer<DefaultProducerContext>) {
    data.iter().enumerate().for_each(|(i, ele)| {
        producer
            .send(
                BaseRecord::to("iceberg-topics")
                    .payload(ele)
                    .key(&format!("Key {}", i))
                    .headers(OwnedHeaders::new().insert(Header {
                        key: "header_key",
                        value: Some("header_value"),
                    })),
            )
            .expect("");
    });
}

async fn future_produce(data: Vec<String>, producer: &FutureProducer) {
    let futures = data
        .iter()
        .enumerate()
        .map(|(i, ele)| async move {
            let deliver_result = producer
                .send(
                    FutureRecord::to("iceberg-topics")
                        .payload(ele)
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                )
                .await;

            return Some(deliver_result);
        })
        .collect::<Vec<_>>();

    join_all(futures).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // let producer: &FutureProducer<_> = &ClientConfig::new()
    let producer: &ThreadedProducer<_> = &ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    // let mut file = File::open("web_requests-100.json")?;
    // let mut contents = String::new();
    // file.read_to_string(&mut contents)?;
    // let content = contents.repeat(1);
    // let con = content.split("\n").into_iter();

    let mut iter_cnt = 0;
    let data = std::iter::from_fn(move || {
        iter_cnt += 1;

        if iter_cnt < 10000 {
            let log_msg = LogMessage::new();
            Some(serde_json::to_string(&log_msg).expect("could not serialize to json"))
        } else {
            None
        }
    })
    .collect::<Vec<String>>();

    let mut cnt = 0;
    let mut interval = time::interval(Duration::from_secs(1));

    tokio::spawn(async {
        let consumer: &BaseConsumer<_> = &ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");

        let mut interval = time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            match consumer.fetch_metadata(None, std::time::Duration::from_secs(5)) {
                Ok(metadata) => {
                    for topic in metadata.topics() {
                        if topic.name() == "iceberg-topics" {
                            if let Ok((low, high)) = consumer.fetch_watermarks(
                                topic.name(),
                                topic.partitions()[0].id(),
                                Duration::from_secs(1),
                            ) {
                                println!("earliest: {:?} and latest: {:?}", low, high);
                            }
                            // .expect("could not fetch watermark");
                        }
                    }
                }
                Err(err) => {
                    println!("fuck this shit! {:?}", err);
                }
            };
        }
    });

    // for _ in 1..2
    loop {
        interval.tick().await;

        let instant = Instant::now();

        cnt += data.len();

        // future_produce(data.clone(), producer).await;
        threaded_producer(data.clone(), producer).await;

        let elapsed_time = instant.elapsed();

        println!(
            "DEBUG: took {:?} to write batch, cnt: {:?}",
            elapsed_time, cnt
        );
    }

    Ok(())
}

// check latency:
// - timestamp: would have to find highest timestamp and what is producer currently writing
// - number: would have to find higher number written nd what is producer currently writing
//  - spark sql
//  - e6data engine

// {"meta":{"producer":{"timestamp":"2021-03-24T15:06:17.321710+00:00"}},"method":"DELETE","session_id":"7c28bcf9-be26-4d0b-931a-3374ab4bb458","status":204,"url":"http://www.youku.com","uuid":"831c6afa-375c-4988-b248-096f9ed101f8"}
// let producer: &FutureProducer = &ClientConfig::new()
// .set("bootstrap.servers", "asdfasdf asdf asdf asdf asdf")
// .set("security.protocol", "SASL_SSL")
// .set("sasl.mechanism", "SCRAM-SHA-512")
// .set("sasl.username", "alice")
// .set("sasl.password", "alice-secret")
// .set("message.timeout.ms", "5000")
// .create()
// .expect("Producer creation error");

// let producer: &FutureProducer = &ClientConfig::new()
//     .set("bootstrap.servers", "alsdjafsdkf")
//     .set("security.protocol", "SASL_SSL")
//     .set("sasl.mechanism", "SCRAM-SHA-512")
//     .set("sasl.username", "admin")
//     .set("sasl.password", "admin")
//     .set("message.timeout.ms", "5000")
//     .create()
//     .expect("Producer creation error");
//
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
// // let data_1 = vec![String::from("2024-11-20T15:00:06.869891203Z stdout F 20-11-2024 15:00:06,869 DEBUG pool-7-thread-1 QueryId= HeartbeatTask:25 - Sending heartbeat from engine: 10.201.109.0 to cluster manager at: debug-test-2-queue")];
// let data_1 = vec![String::from("2024-11-20T15:04:55.546150892Z stdout F 20-11-2024 15:04:55,546 DEBUG qtp571514712-227 QueryId= ExecutorHttpService:34 - Received request on engine http server: Path: /, Method: GET")];
// // let data_n = data_1.repeat(100);

// produce(data_1).await;
//
//
// // let producer: &FutureProducer = &ClientConfig::new()
// //     .set("bootstrap.servers", "localhost:9092")
// //     .set("message.timeout.ms", "5000")
// //     .create()
// //     .expect("Producer creation error");
//
//
// // let topic_name = "iceberg-topics";
// // let num_partitions = 3;
// // let replication_factor = 1;
//
// // let new_topic = NewTopic::new(
// //     topic_name,
// //     num_partitions,
// //     TopicReplication::Fixed(replication_factor),
// // );
//
// // match consumer.create_topics(vec![&new_topic], &AdminOptions::new()).await {
// //     Ok(results) => {println!("results: {:?}", results);}
// //     Err(err) => {println!("err: {:?}", err);}
// // };
//
// let (version_n, version_s) = get_rdkafka_version();
// println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
//
// // match consumer.fetch_metadata(None, std::time::Duration::from_secs(5)) {
// //     Ok(metadata) => {
// //         for topic in metadata.topics() {
// //             println!("topic name: {:?}", topic.name());
// //         }
// //     },
// //     Err(err) => {
// //         println!("fuck this shit! {:?}", err);
// //     },
// // };
//
// // Ok(())
