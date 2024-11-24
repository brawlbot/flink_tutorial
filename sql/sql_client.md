# Create table


```sh
docker-compose exec flink-sql-client /bin/bash -c "./sql-client.sh"
```

```sql
DROP TABLE IF EXISTS user_behavior;

CREATE TABLE user_behavior (
    role_id BIGINT,
    behavior STRING,
    -- ts TIMESTAMP(3),
    -- proctime AS PROCTIME(),   -- generates processing-time attribute using computed column
    -- WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- defines watermark on ts column, marks ts as event-time attribute
) WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'user_behavior',  -- kafka topic
    'scan.startup.mode' = 'earliest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = '10.237.96.122:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
);

select * from user_behavior;
```

# Test network

```bash
docker run -it --rm --network=host nicolaka/netshoot /bin/bash -c "telnet 10.237.96.122 9092"
docker run -it --rm --network=host nicolaka/netshoot /bin/bash -c "telnet 127.0.0.1 9092"
```

```bash
docker run -it --rm --network=host wurstmeister/kafka:2.12-2.2.1 /bin/bash

/opt/kafka_2.12-2.2.1/bin/kafka-console-consumer.sh --topic user_behavior --bootstrap-server 10.237.96.122:9092 --from-beginning --max-messages 10
/opt/kafka_2.12-2.2.1/bin/kafka-console-consumer.sh --topic user_behavior --bootstrap-server 127.0.0.1:9092 --from-beginning --max-messages 10


/bin/kafka-run-class kafka.tools.ConsoleConsumer --topic user_behavior --bootstrap-server 127.0.0.1:9092 --from-beginning --max-messages 10

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx512M"
fi

exec $(dirname $0)/kafka-run-class.sh kafka.tools.ConsoleConsumer "$@"

kafka-run-class.sh
```