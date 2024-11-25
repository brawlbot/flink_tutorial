# Create table
```sh
SHOW TABLES;
```
## watermark
```sql
DROP TABLE IF EXISTS user_behavior;

CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3),
    proctime AS PROCTIME(),   -- generates processing-time attribute using computed column
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- defines watermark on ts column, marks ts as event-time attribute

) WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'user_behavior',  -- kafka topic
    'scan.startup.mode' = 'latest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = '10.237.96.122:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
);

SELECT * FROM user_behavior;
```
Explain tolerate 5-seconds out-of-order, ts field becomes an event-time attribute

# Sql
## Aggregate
```sql

SELECT 
    user_id, 
    COUNT(*) AS behavior_count
FROM 
    user_behavior
GROUP BY 
    user_id;

EXPLAIN PLAN FOR SELECT role_id, count(*) from user_behavior group by role_id;
EXPLAIN CHANGELOG_MODE FOR SELECT role_id, count(*) from user_behavior group by role_id;

```

# Test network

```bash
docker run -it --rm --network=host nicolaka/netshoot /bin/bash -c "telnet 10.237.96.122 9092"
docker run -it --rm --network=host nicolaka/netshoot /bin/bash -c "telnet 127.0.0.1 9092"
```


```sql
DROP TABLE IF EXISTS buy_cnt_per_second;

CREATE TABLE buy_cnt_per_second (
    -- hour BIGINT,
    second_of_minue BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'buy_cnt_per_second',  -- kafka topic
    'scan.startup.mode' = 'latest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = '10.237.96.122:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
);


INSERT INTO buy_cnt_per_second
SELECT SECOND(TUMBLE_START(ts, INTERVAL '1' SECOND)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' SECOND);

SELECT * FROM buy_cnt_per_second;
-- SELECT CAST('2024-11-25 01:28:00' AS TIMESTAMP);
```


### Group by item
```sql
DROP TABLE IF EXISTS buy_cnt_per_item;

CREATE TABLE buy_cnt_per_item (
    item BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'buy_cnt_per_item',  -- kafka topic
    'scan.startup.mode' = 'latest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = '10.237.96.122:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
    -- 'connector' = 'elasticsearch-7', -- using elasticsearch connector
    -- 'hosts' = 'http://elasticsearch:9200',  -- elasticsearch address
    -- 'index' = 'buy_cnt_per_hour'  -- elasticsearch index name, similar to database table name
);

```

### elastic search
```sql
DROP TABLE IF EXISTS buy_cnt_per_second_elk;

CREATE TABLE buy_cnt_per_second_elk (
    -- hour BIGINT,
    second_of_minue BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = '10.237.96.122:9200',  -- elasticsearch address
    'index' = 'buy_cnt_per_second_elk'  -- elasticsearch index name, similar to database table name
);

INSERT INTO buy_cnt_per_second_elk
SELECT SECOND(TUMBLE_START(ts, INTERVAL '1' SECOND)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' SECOND);


select * from buy_cnt_per_second_elk;

INSERT INTO buy_cnt_per_second_elk VALUES (1, 2);

```

# Reference

document of function sql be found in [systemfunctions](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/functions/systemfunctions/)



# Elasticsearch

flink-sql-connector-elasticsearch7_2.11


```sql

CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3),
    proctime AS PROCTIME(),   -- generates processing-time attribute using computed column
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- defines watermark on ts column, marks ts as event-time attribute

) WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'user_behavior',  -- kafka topic
    'scan.startup.mode' = 'latest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = '10.237.96.122:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
);

-- DROP TABLE IF EXISTS buy_cnt_per_second_elk;

CREATE TABLE buy_cnt_per_second_elk (
    -- hour BIGINT,
    second_of_minue BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = '10.237.96.122:9200',  -- elasticsearch address
    'index' = 'buy_cnt_per_second_elk'  -- elasticsearch index name, similar to database table name
);



INSERT INTO buy_cnt_per_second_elk
SELECT SECOND(TUMBLE_START(ts, INTERVAL '1' SECOND)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' SECOND);


select * from  buy_cnt_per_second_elk;