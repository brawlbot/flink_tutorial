package kafka;
//package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.AggregateFunction;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
/**
 * This is a re-write of the Apache Flink WordCount example using Kafka connectors.
 * Find the original example at 
 * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/wordcount/WordCount.java
 */
public class WordCount {

	final static String inputTopic = "input-topic";
	final static String outputTopic = "output-topic";
	final static String jobTitle = "WordCount";

	public static void main(String[] args) throws Exception {
	    final String bootstrapServers = args.length > 0 ? args[0] : "10.237.96.122:9092";
	    
//	    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> source = KafkaSource.<String>builder()
		    .setBootstrapServers(bootstrapServers)
		    .setTopics(inputTopic)
		    .setGroupId("my-group")
		    .setStartingOffsets(OffsetsInitializer.latest())
		    .setValueOnlyDeserializer(new SimpleStringSchema())
		    .build();

		KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
			.setValueSerializationSchema(new SimpleStringSchema())
			.setTopic(outputTopic)
			.build();

		KafkaSink<String> sink = KafkaSink.<String>builder()
			.setBootstrapServers(bootstrapServers)
			.setRecordSerializer(serializer)
			.build();

		DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		// Split up the lines in pairs (2-tuples) containing: (word,1)

		
		// Group by the tuple field "0" and sum up tuple field "1"

//		DataStream<String> counts = text.flatMap(new Tokenizer())
//		.keyBy(value -> value.f0)
////		.window(TumblingEventTimeWindows.of(Time.seconds(1))) // Define a tumbling window if required
//		.sum(1)
//		.flatMap(new Reducer());
        
        
//        DataStream<String> counts = text.flatMap(new Tokenizer())
//	        .keyBy(value -> value.f0)
//	        .window(TumblingEventTimeWindows.of(Time.seconds(1))) // Define a tumbling window if required
//	        .aggregate(new WordCountAggregateFunction()) // Use the custom aggregate function
//	        .flatMap(new Reducer());
        
        
        DataStream<String> counts = text.flatMap(new Tokenizer())
        	    .keyBy(value -> value.f0)
//        	    .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        	    .reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + 2*value2.f1))
        	    .flatMap(new Reducer());
       
        counts.sinkTo(sink);
		// Execute program
		env.execute(jobTitle);
	}

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

			// std output
			System.out.println(value);
            // Emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    // Implements a simple reducer using FlatMap to
    // reduce the Tuple2 into a single string for 
    // writing to kafka topics
    public static final class Reducer
            implements FlatMapFunction<Tuple2<String, Integer>, String> {

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<String> out) {
        	// Convert the pairs to a string
        	// for easy writing to Kafka Topic
        	String count = value.f0 + "====" + value.f1;
        	out.collect(count);
        }
    }
    
    
    public static class WordCountAggregateFunction
    implements AggregateFunction<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>> {
	
		@Override
		public Integer createAccumulator() {
		    return 100; // Initial accumulator value
		}
		
		@Override
		public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
		    return 100* accumulator + 10 * value.f1; // Increment the count
		}
		
		@Override
		public Tuple2<String, Integer> getResult(Integer accumulator) {
		    return new Tuple2<>("word", 2*accumulator); // Produce the result
		}
		
		@Override
		public Integer merge(Integer a, Integer b) {
		    return a + b; // Combine accumulators
		}
	}
}