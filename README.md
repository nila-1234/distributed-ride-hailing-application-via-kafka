# Distributed Ride-Hailing Application via Kafka

A distributed, stream-processing simulation of a NYC-based ride-hailing platform, built on **Apache Kafka** and **Apache Samza** and deployed to an **EMR/Hadoop cluster**. Raw trip, driver, and ad-click events are produced onto Kafka topics and processed in real time by a set of Samza stream tasks.

## Architecture

```
DataProducer  --->  Kafka topics  --->  Samza stream tasks  --->  output topics
```

**stream-processing/DataProducer** — reads a trace file of simulated events (rider requests, driver location updates, ad clicks) and publishes them onto the appropriate Kafka topics, partitioned by user/block ID.

**driver-match** — consumes the driver-location and rider-request streams and joins them to produce a stream of rider-to-driver matches.

**ad-match** — consumes rider events together with static Yelp business data and per-user profile info, and outputs a stream of matched in-app advertisements for each rider.

**ad-price** — consumes the ad-click stream, combines it with static ad-info data, and outputs a stream of ad revenue distribution.

Each of the three Samza modules (`driver-match`, `ad-match`, `ad-price`) is an independent Maven project with its own `pom.xml`, deploy/submitter scripts for running the job on the cluster, and a `start_kafka.sh` script for bringing up Kafka/Samza on EMR.

## Tech stack

- Apache Kafka — event streaming backbone
- Apache Samza — stateful stream processing (keyed KV stores per task)
- AWS EMR/Hadoop — cluster deployment
- Java + Maven

## Project layout

```
stream-processing/DataProducer/   # synthetic event producer
driver-match/driver-match/        # rider <-> driver matching Samza job
ad-match/ad-match/                # rider <-> ad matching Samza job
ad-price/ad-price/                # ad revenue distribution Samza job
```

## Running

Each module ships its own `start_kafka.sh` (cluster bring-up), `deploy_*` / `submitter_*` scripts (job submission), and `runner.sh` (job execution) for deploying to an EMR-hosted Kafka/Samza cluster. See each module's `pom.xml` for build details.
