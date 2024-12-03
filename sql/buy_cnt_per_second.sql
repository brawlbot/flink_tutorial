-- DROP TABLE IF EXISTS user_behavior;

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


-- DROP TABLE IF EXISTS buy_cnt_per_second_elk;

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