-- DROP TABLE IF EXISTS user_behavior;

CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id STRING,
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



-- DROP TABLE IF EXISTS category_dim;

-- 'url' = 'jdbc:mysql://10.237.96.122:3306/flink',
CREATE TABLE category_dim (
    sub_category_id STRING,
    parent_category_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://10.237.96.122:3306/flink?useSSL=false&serverTimezone=UTC&connectTimeout=30000',
    'table-name' = 'category',
    'username' = 'root',
    'password' = '123456',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '10s'
);


-- select * from category_dim;


-- DROP TABLE IF EXISTS top_category;
CREATE TABLE top_category (
    category_name STRING PRIMARY KEY NOT ENFORCED,
    buy_cnt BIGINT
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = '10.237.96.122:9200',
    'index' = 'top_category'
);


-- DROP VIEW IF EXISTS rich_user_behavior;
CREATE VIEW rich_user_behavior AS
SELECT U.user_id, U.item_id, U.behavior, C.parent_category_name as category_name
FROM user_behavior AS U LEFT JOIN category_dim FOR SYSTEM_TIME AS OF U.proctime AS C
ON U.category_id = C.sub_category_id;


INSERT INTO top_category
SELECT category_id, COUNT(*) buy_cnt
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY category_id;
-- FROM rich_user_behavior