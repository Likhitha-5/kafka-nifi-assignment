# Kafka and NiFi Assignment Report

---

## 1. Environment

My work laptop enforces an App Control for Business policy that blocks Docker Desktop installation and requires administrator rights for WSL 2. I therefore ran Docker, Kafka, and NiFi in a GitHub Codespace, which provides a Linux environment with Docker preinstalled.

- Codespace: created from the `kafka-nifi-assignment` repository
- Docker and Docker Compose: available in the Codespace
- NiFi: `apache/nifi:2.0.0`, run as a container on the `kafka-network` created by Docker Compose

## 2. Install NiFi

NiFi was started with:

```bash
docker run -d --name nifi --network kafka-network -p 8443:8443 \
  -e SINGLE_USER_CREDENTIALS_USERNAME=admin \
  -e SINGLE_USER_CREDENTIALS_PASSWORD=<password> \
  apache/nifi:2.0.0
```

The NiFi UI was opened through the Codespace's forwarded port 8443 and logged in as `admin`.

## 3. Install Docker and start Kafka

Docker was not installed on the work laptop because of the policy above, so it was used in the Codespace. Kafka was started from the provided `Kafka` folder:

```bash
cd Kafka
docker compose up -d
```

Zookeeper and Kafka reported **Healthy**, and the Kafka UI (port 8080) showed the `local` cluster online. See `screenshots/02_docker_compose_up_healthy.png` and `screenshots/04_kafka_ui_local_cluster.png`.

The topic was created with 3 partitions:

```bash
docker exec kafka-kafka-1 kafka-topics --create --topic user-events \
  --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

Verified with `--describe`: `PartitionCount: 3`, `ReplicationFactor: 1`, partitions 0, 1, and 2 all led by broker 1. See `screenshots/03_kafka_topic_3_partitions.png`.

**Issues encountered and fixes:**
- The first `docker compose up` failed with "container with given ID already exists" because of stale containers. Fixed by running `docker compose down` and removing the old containers.
- Kafka reported unhealthy on one start. Fixed by restarting the stack once the stale containers were removed.
- Running `docker compose` from the repository root gave "no configuration file provided". Fixed by running it from the `Kafka` folder.
- The Codespace restarted several times, which stopped the containers. Fixed by restarting them with `docker start` and `docker compose up -d`.

## 4. Producer and consumer

The provided `producer_demo.py` sends one event per second to `user-events`, keyed by user ID. The provided `consumer_demo.py` reads the topic and prints each message with a dashboard summary every 10 messages. Both ran successfully, and the consumer displayed messages from all three partitions.

## 5. NiFi data flow

The flow is inside the processor group **Kafka Consumer Demo**.

| Component | Type | Purpose |
|---|---|---|
| ConsumeKafka | ConsumeKafka 2.0.0 | Reads messages from `user-events` through a Kafka Connection Service |
| PutFile | PutFile 2.0.0 | Writes each message to a file (success path) |
| LogAttribute | LogAttribute 2.0.0 | Receives PutFile's failure connection |

**Configuration:**
- Kafka Connection Service (Kafka3ConnectionService): Bootstrap Servers `kafka:29092`, Security Protocol `PLAINTEXT`
- ConsumeKafka: Topics `user-events`, Group ID `nifi-consumer`, Auto Offset Reset `earliest`
- PutFile: Directory `/opt/nifi/nifi-current/output`, Create Missing Directories `true`
- Connections: ConsumeKafka **success** to PutFile; PutFile **failure** to LogAttribute
- PutFile's **success** relationship is auto-terminated, since files are the final output

**Why `kafka:29092`:** NiFi runs in its own container on the same Docker network, so it reaches Kafka by container name and the internal listener port. `localhost:9092` works only from the host.

See `screenshots/08_nifi_flow_canvas.png` for the flow and `screenshots/09_consumekafka_settings.png` for the ConsumeKafka configuration.

## 6. Results

- ConsumeKafka read messages from all three partitions of `user-events`.
- PutFile wrote each message as a separate file. At the time of export, the output folder contained **775 files**.
- Each file holds one JSON event with `user_id`, `event`, `page`, `timestamp`, `session_id`, and `duration_ms`. Example:

```json
{"user_id": "user_002", "event": "purchase", "page": "/home", "timestamp": "2026-11-01T04:49:40.385709", "session_id": "session_4649", "duration_ms": 3583}
```

- A sample of 20 output files is included in `output/`.

## 7. Failure path

The failure path is configured: PutFile's failure relationship is connected to LogAttribute, so any flowfile PutFile cannot write would be routed there and recorded.

## 8. Submission contents

- `Assigment.md` and `Kafka/`: the provided files
- `nifi/kafka-consumer-demo.json`: exported flow definition
- `output/`: sample of saved messages
- `screenshots/`: Docker and Kafka status, topic description, Kafka UI, NiFi flow canvas, and ConsumeKafka settings
- `REPORT.md`: this report
