# Setup
```sh
wget https://raw.githubusercontent.com/confluentinc/cp-all-in-one/refs/heads/7.5.0-post/cp-all-in-one-flink/docker-compose.yml
docker compose up -d
docker exec -it flink-sql-client sql-client.sh
```

# Tunel
```sh
# list topic
docker exec -it broker kafka-topics --bootstrap-server broker:29092 --list
```
# Usage
## create topic
```sql
DROP TABLE IF EXISTS KafkaTable;
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
);
SELECT * FROM KafkaTable;
```

## Upsert
```sql
CREATE TABLE pageviews_per_region (
  user_region STRING,
  pv BIGINT,
  uv BIGINT,
  PRIMARY KEY (user_region) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'pageviews_per_region',
  'properties.bootstrap.servers' = '...',
  'key.format' = 'avro',
  'value.format' = 'avro'
);

CREATE TABLE pageviews (
  user_id BIGINT,
  page_id BIGINT,
  viewtime TIMESTAMP,
  user_region STRING,
  WATERMARK FOR viewtime AS viewtime - INTERVAL '2' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'pageviews',
  'properties.bootstrap.servers' = '...',
  'format' = 'json'
);

-- calculate the pv, uv and insert into the upsert-kafka sink
INSERT INTO pageviews_per_region
SELECT
  user_region,
  COUNT(*),
  COUNT(DISTINCT user_id)
FROM pageviews
GROUP BY user_region;

```

# Detail of service and configuration
## tunnel
```sh
ssh -N -L 9081:localhost:9081 demo@10.237.96.122
```
## service
| service | port | url | note |
| --- | --- | --- | --- |
| flink dashboard | 9081 | http://localhost:9081 | |
| kafka control-center | 9021 | http://localhost:9021 | |
| kafka broker | 9092 | | |