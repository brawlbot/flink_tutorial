# Download jars dependence
In `pom.xml`, add dependencies for Elasticsearch and Kafka. For example, you can refer to the following file:
    - [elastic_pom.xml](../jars/elastic_pom.xml)
    - [kafka_pom.xml](../jars/kafka_pom.xml)
    - [jdbc_pom.xml](../jars/jdbc_pom.xml)
To install jars to local folder `jars`, run the following command:
```sh
mvn dependency:copy-dependencies -DoutputDirectory=./elastic -f elastic_pom.xml
mvn dependency:copy-dependencies -DoutputDirectory=./kafka -f kafka_pom.xml
mvn dependency:copy-dependencies -DoutputDirectory=./jdbc -f jdbc_pom.xml
```
## Run sql in local mode (Option 1)
In docker container, run the following command:
```sh
docker-compose exec flink-sql-client /bin/bash

./sql-client.sh \
    -l /mnt/jars/elastic \
    -l /mnt/jars/kafka \
    -l /mnt/jars/jdbc

export HADOOP_CLASSPATH=`hadoop classpath`
```
## Run sql in hadoop (Option 2)
In docker container, run the following command:

```sh
export HADOOP_CLASSPATH=`hadoop classpath`

cd ${HOME}/flink-1.16.0

./bin/sql-client.sh \
    -j ${HOME}/.m2/repository/org/apache/flink/flink-connector-kafka/1.16.0/flink-connector-kafka-1.16.0.jar \
    -j ${HOME}/.m2/repository/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar \
    -l ${HOME}/jars/elasticsearch \
    -l ${HOME}/jars/jdbc

```


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
    category_id STRING,
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
```

```sql
SELECT * FROM user_behavior;
```
Then the output will be:
![cli_user_behavior](../image/cli_user_behavior.png)


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

Query the data in kafka

![cli_buy_cnt_per_second](../image/cli_buy_cnt_per_second.png)

Or in Kafka Control Center

![kafka_buy_cnt_per_second](../image/kafka_buy_cnt_per_second.png)


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

Delete the index before creating the table
```sh
curl -X DELETE "http://10.237.96.122:9200/buy_cnt_per_second"
```

```sql
DROP TABLE IF EXISTS buy_cnt_per_second_elk;

CREATE TABLE buy_cnt_per_second_elk (
    -- hour BIGINT,
    second_of_minue BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = '10.237.96.122:9200',  -- elasticsearch address
    'index' = 'buy_cnt_per_second'  -- elasticsearch index name, similar to database table name
);

INSERT INTO buy_cnt_per_second_elk
SELECT SECOND(TUMBLE_START(ts, INTERVAL '1' SECOND)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' SECOND);

-- INSERT INTO buy_cnt_per_second_elk VALUES (1, 2);
```

# Reference

document of function sql be found in [systemfunctions](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/functions/systemfunctions/)


# get cumulative uv

each 10 seconds, get the cumulative uv
```sh
curl -X DELETE "http://10.237.96.122:9200/cumulative_uv_10sec"
```

```sql
DROP TABLE IF EXISTS cumulative_uv_10sec;

CREATE TABLE cumulative_uv_10sec (
    date_str STRING,
    time_str STRING,
    uv BIGINT
    -- PRIMARY KEY (date_str, time_str) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = '10.237.96.122:9200',  -- elasticsearch address
    'index' = 'cumulative_uv_10sec'
);


INSERT INTO cumulative_uv_10sec
SELECT date_str, MAX(time_str), COUNT(DISTINCT user_id) as uv
FROM (
  SELECT
    DATE_FORMAT(ts, 'yyyy-MM-dd') as date_str,
    CONCAT(SUBSTR(DATE_FORMAT(ts, 'HH:mm:ss'), 1, 7), '0') as time_str,
    user_id
  FROM user_behavior
)
GROUP BY date_str;

```

output in Kibana
![kibana_cumulative_uv_10sec](../image/kibana_cumulative_uv_10sec.png)

# Get total unique user

```sh
curl -X DELETE "http://10.237.96.122:9200/total_uv_10sec"
```

```sql
DROP TABLE IF EXISTS total_uv_10sec;

CREATE TABLE total_uv_10sec (
    date_str STRING,
    time_str STRING,
    uv BIGINT,
    PRIMARY KEY (date_str, time_str) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = '10.237.96.122:9200',  -- elasticsearch address
    'index' = 'total_uv_10sec'
);


INSERT INTO total_uv_10sec
SELECT date_str, MAX(time_str), COUNT(DISTINCT user_id) as uv
FROM (
  SELECT
    DATE_FORMAT(ts, 'yyyy-MM-dd') as date_str,
    CONCAT(SUBSTR(DATE_FORMAT(ts, 'HH:mm:ss'), 1, 7), '0') as time_str,
    user_id
  FROM user_behavior
)
GROUP BY date_str;
```

output in Kibana
![kibana_total_uv_10sec](../image/kibana_total_uv_10sec.png)


# Top category

connect to mysql 10.237.96.122:3306

```sql
DROP TABLE IF EXISTS category_dim;

CREATE TABLE category_dim (
    sub_category_id STRING,
    parent_category_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://10.237.96.122:3306/flink',
    'table-name' = 'category',
    'username' = 'root',
    'password' = '123456',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '10s'
);

SELECT * FROM category_dim;
```


```sql
-- DROP TABLE IF EXISTS top_category;
CREATE TABLE top_category (
    category_name STRING PRIMARY KEY NOT ENFORCED,
    buy_cnt BIGINT
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = '10.237.96.122:9200',
    'index' = 'top_category'
);
```

output in Kibana
![kibana_top_category](../image/kibana_top_category.png)


# Overview the dashboard
![dashboard](../image/dashboard.png)