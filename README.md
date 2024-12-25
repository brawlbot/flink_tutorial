# Preinstall

## 1. Change host
setup with [docker-compose.yml](docker-compose.yml) with command assume this deployment is in Host 10.237.96.122
in this repo using host 10.237.96.122 as the host so you can change it to localhost (127.0.0.1) by running the command below to apply all project

example:
```sh
git clone https://github.com/brawlbot/flink_tutorial && cd flink_tutorial
find . -type f \( -name "*.sql" -o -name "*.ipynb" -o -name "*.yml" -o -name "*.java" \) -exec sed -i 's/10.237.96.122/10.237.96.145/g' {} +
```
## 2. Initialize dependency

- Java 1.18 with Maven
- Docker

## 3. Install mvn
```sh
wget https://dlcdn.apache.org/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz
tar -xvf apache-maven-3.9.9-bin.Dtar.gz
mv apache-maven-3.9.9 /opt/
M2_HOME='/opt/apache-maven-3.9.9'
PATH="$M2_HOME/bin:$PATH"
export PATH
```


# Deployment



then run the command below
```sh
docker compose up -d
```
then list of service
|Service| URL | Description |
|---|---|---|
|Kafka| [http://10.237.96.122:9021/](http://10.237.96.122:9021/) | Kafka Control Center |
|Flink Dashboard| [http://10.237.96.122:9081/](http://10.237.96.122:9081/) | Note that it implement localhost |
|Kibana| [http://10.237.96.122:5601/](http://10.237.96.122:5601/) | Tool to visualize elastic search data |
|Elastic Search| [http://10.237.96.122:9200/](http://10.237.96.122:9200/) | Elastic search |
|Yarn UI| [http://node8.lab.internal:8088/cluster/apps/RUNNING](http://node8.lab.internal:8088/cluster/apps/RUNNING) | Optional: Yarn UI |

# wordcount

Context of this project:
this project is to learn Flink by implementing a simple wordcount application, setting configuration and monitoring the job.

## 0. get env
- `env.getConfig().toString();`

## 1. parallelism

- set parallelism by `env.setParallelism(2);`
- check parallelism by `System.out.println(text.getExecutionConfig().toString());`

## 2. file source
```java
builder.monitorContinuously(Duration.ofMillis(1));
```
## 3. Usage

create a new file in `input` folder
```bash
echo "w8" > input/input8.txt
```

and the result will be like this

```log
[SourceCoordinator-Source: file-input] INFO org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner - Assigning split to non-localized request: Optional[FileSourceSplit: file:/Users/lap15143/RnD/flink/wordcount/input/input8.txt [0, 3) (no host info) ID=0001522441 position=null]
[Source: file-input -> tokenizer (1/2)#0] INFO org.apache.flink.connector.base.source.reader.SourceReaderBase - Adding split(s) to reader: [FileSourceSplit: file:/Users/lap15143/RnD/flink/wordcount/input/input8.txt [0, 3) (no host info) ID=0001522441 position=null]
[Source Data Fetcher for Source: file-input -> tokenizer (1/2)#0] INFO org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher - Starting split fetcher 2
[Source Data Fetcher for Source: file-input -> tokenizer (1/2)#0] INFO org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher - Finished reading from splits [0001522441]
w8
[Source: file-input -> tokenizer (1/2)#0] INFO org.apache.flink.connector.base.source.reader.SourceReaderBase - Finished reading split(s) [0001522441]
[Source: file-input -> tokenizer (1/2)#0] INFO org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager - Closing splitFetcher 2 because it is idle.
[Source: file-input -> tokenizer (1/2)#0] INFO org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher - Shutting down split fetcher 2
[Source Data Fetcher for Source: file-input -> tokenizer (1/2)#0] INFO org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher - Split fetcher 2 exited.
[SourceCoordinator-Source: file-input] INFO org.apache.flink.runtime.source.coordinator.SourceCoordinator - Source Source: file-input received split request from parallel task 0 (#0)
```


# Multiple file source
- `FileSource.forRecordStreamFormat(new TextLineInputFormat(), input_filePath);`
- `builder.monitorContinuously(Duration.ofMillis(1));`
- `builder.monitorContinuously(Duration.ofSeconds(10));`


# Notable Documentation
- [Flink SQL](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/)
# kill all process contain `wordcount`
```bash
kill $(ps aux | grep wordcount | grep -v grep | awk '{print $2}')
```

# Flink run to yarn 


Refer to [yarn/setup.md](yarn/setup.md)

summary of the steps
1. run example
2. savepoint


# Flink SQL Client
## Generate data
to generate data for testing, refer to [sql/producer.ipynb](sql/producer.ipynb)

Refer to [sql/sql_client.md](sql/sql_client.md)

Summary of the steps
1. Download jars dependence
2. Run sql client in local mode or hadoop mode
3. Run sql statement
    - [buy_cnt_per_second.sql](sql/buy_cnt_per_second.sql)
    - [top_category.sql](sql/top_category.sql)