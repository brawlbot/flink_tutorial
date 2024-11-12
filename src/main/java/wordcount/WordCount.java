package wordcount;

// Setting built-in
import java.util.logging.*; 
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// env setting
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.MemorySize;


// connector
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;

import org.apache.flink.streaming.api.datastream.DataStream;


// org.apache.flink connector flink-connector-files
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;


//serialization
import org.apache.flink.api.common.serialization.SimpleStringEncoder;


// org flink java
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FlatMapFunction;

// utils
import org.apache.flink.util.Collector;



public class WordCount {
    public static void main(String[] args) throws Exception {

        Logger logger = Logger.getLogger("Logging");  
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // set env mode:  STREAMING, BATCH, AUTOMATIC
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        FileHandler fh;   
        try {  
  
            fh = new FileHandler("logs/echo.log");  
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();  
            fh.setFormatter(formatter);  
  
            logger.info("Start logging mode: " + RuntimeExecutionMode.STREAMING);  

        } catch (SecurityException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } 
                // .setProperty("partition.discovery.interval.ms", "10000");
        ExecutionConfig executionConfig = env.getConfig();


        logger.info(executionConfig.getClass().toString());
        
        logger.info(executionConfig.toString());
        DataStream<String> text;
        Path input_filePath = new Path("input/wordcount.txt");
        
        FileSource.FileSourceBuilder<String> builder =
            FileSource.forRecordStreamFormat(new TextLineInputFormat(), input_filePath);
        
//        builder.monitorContinuously(Duration.ofMillis(1));
        builder.monitorContinuously(Duration.ofSeconds(10));
        
        logger.info("builder.getClass().toString() = " + builder.getClass().toString());

        text = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "file-input");

        
        DataStream<Tuple2<String, Integer>> counts =
                // The text lines read from the source are split into words
                // using a user-defined function. The tokenizer, implemented below,
                // will output each word as a (2-tuple) containing (word, 1)
                text.flatMap(new Tokenizer())
                        .name("tokenizer")
                        // keyBy groups tuples based on the "0" field, the word.
                        // Using a keyBy allows performing aggregations and other
                        // stateful transformations over data on a per-key basis.
                        // This is similar to a GROUP BY clause in a SQL query.
                        .keyBy(value -> value.f0)
                        // For each key, we perform a simple sum of the "1" field, the count.
                        // If the input data stream is bounded, sum will output a final count for
                        // each word. If it is unbounded, it will continuously output updates
                        // each time it sees a new instance of each word in the stream.
                        .sum(1)
                        .name("counter");

        counts.sinkTo(
                FileSink.<Tuple2<String, Integer>>forRowFormat(
                                new Path("output"), new SimpleStringEncoder<>())
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder()
                                        .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                        .withRolloverInterval(Duration.ofSeconds(10))
                                        .build())
                        .build())
            .name("file-sink");
        
        logger.info("Prepair exec");
        
        env.execute("WordCount");
        
        // logger.info("Done");
        
    }

    public static final class Tokenizer
	        implements FlatMapFunction<String, Tuple2<String, Integer>> {
	
	    @Override
	    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
	        // normalize and split the line
	        String[] tokens = value.toLowerCase().split("\\W+");
	
	        // emit the pairs
	        for (String token : tokens) {
	            if (token.length() > 0) {
	                out.collect(new Tuple2<>(token, 1));
	            }
	        }
	    }
	}
}