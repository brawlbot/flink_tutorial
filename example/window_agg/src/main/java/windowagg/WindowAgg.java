package windowagg;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

// import kafka.WordCount.Reducer;

public class WindowAgg {

	final static String inputTopic = "user_behavior";
	final static String outputTopic = "output-topic";
	final static String jobTitle = "UserBehavior";

	public static void main(String[] args) throws Exception {
	    final String bootstrapServers = args.length > 0 ? args[0] : "10.237.96.122:9092";

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final UserBehavior ub = new UserBehavior(1, 1, "acb", "buy", "2024-11-26 15:56:19.213");
//		(long user_id, long item_id, String category_id, String behavior, String ts)
		// final JSONDeserializationSchema abc= new JSONDeserializationSchema<>(UserBehavior.class)
        KafkaSource<UserBehavior> source = KafkaSource.<UserBehavior>builder()
			.setBootstrapServers(bootstrapServers)
    		.setTopics(inputTopic)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(
            		new JsonDeserializationSchema<>(UserBehavior.class)
    		)
            .build();
        
        // watermark
        DataStreamSource<UserBehavior> UserBehaviorStream = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "user_source");
        
        // KafkaRecordSerializationSchema<UserStatistics> statisticsSerializer = KafkaRecordSerializationSchema.<UserStatistics>builder()
        //         .setTopic("userstatistics")
        //         .setValueSerializationSchema(new JsonSerializationSchema<>(
        //                 () -> new ObjectMapper().registerModule(new JavaTimeModule())
        //         ))
        //         .build();

        
        // // define sink
        // KafkaSink<UserStatistics> statsSink = KafkaSink.<UserStatistics>builder()
        // 		.setBootstrapServers(bootstrapServers)
        //         .setRecordSerializer(statisticsSerializer)
        //         .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        //         .build();

		KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
			.setValueSerializationSchema(new SimpleStringSchema())
			.setTopic(outputTopic)
			.build();
		
		KafkaSink<String> statsSink = KafkaSink.<String>builder()
			.setBootstrapServers(bootstrapServers)
			.setRecordSerializer(serializer)
			.build();

		// DataStream<String> reducedStream = UserBehaviorStream
		// 	.keyBy(userBehavior -> userBehavior.getBehavior())
		// 	.sum(1)
		// 	.flatMap(new Reducer());
		// DataStream<UserStatistics> reducedStream = UserBehaviorStream
//		UserBehaviorStream
//		DataStream<Tuple2<String, Integer>> reducedStream = UserBehaviorStream
//			.filter(userBehavior -> userBehavior.getBehavior().equals("buy")) // Filter for "buy" behavior
//			.keyBy(userBehavior -> userBehavior.getCategory_id()) // Group by category_id
//			.aggregate(new SumAggregateFunction()) // Ensure the return type matches
//			.flatMap(new Reducer()); // Apply the reducer to convert to string for Kafka
		
		
//		DataStream<Tuple2<String, Integer>> reducedStream = UserBehaviorStream
//			    .filter(userBehavior -> userBehavior.getBehavior().equals("buy")) // Filter for "buy" behavior
//			    .keyBy(userBehavior -> userBehavior.getCategory_id()) // Group by category_id
//			    .window(TumblingEventTimeWindows.of(Time.seconds(1))) // Define a tumbling window of 1 second	
//		    	.aggregate(new SumAggregateFunction()); // Use the corrected aggregate function

		
		DataStream<String> reducedStream = UserBehaviorStream
			    .filter(userBehavior -> userBehavior.getBehavior().equals("buy")) // Filter for "buy" behavior
			    .keyBy(userBehavior -> userBehavior.getCategory_id()) // Group by category_id
			    .reduce(
			    		(value1, value2) -> {
                            value1.set_agg(value1.get_agg() + 1); // Increment total_agg
                            return value1; // Return the updated value1
                        })
			    .flatMap(new Reducer());
//
		reducedStream.sinkTo(statsSink);
		
//		System.out.println(ub.getBehavior().equals("buy"));
		System.out.println(ub.getBehavior());
		System.out.println(ub);
		
		env.execute(jobTitle);
        System.out.println("Done");

    // Implements a simple reducer using FlatMap to
    // reduce the Tuple2 into a single string for 
    // writing to kafka topics
	}
	
//    public static final class Reducer
//            implements FlatMapFunction<Tuple2<String, Integer>, String> {
//
//        @Override
//        public void flatMap(Tuple2<String, Integer> value, Collector<String> out) {
//        	// Convert the pairs to a string
//        	// for easy writing to Kafka Topic
//        	String count = value.f0 + " " + value.f1;
//        	out.collect(count);
//        }
//    }
	  public static final class Reducer
	  implements FlatMapFunction<UserBehavior, String> {
	
		@Override
		public void flatMap(UserBehavior value, Collector<String> out) {
			// Convert the pairs to a string
			// for easy writing to Kafka Topic
            System.out.println("UserBehavior: " + value);
			String describe = value.getCategory_id() + " " + value.get_agg();
			out.collect(describe);
		}
	  }

    
	//    AggregateFunction
	//    IN - The type of the values that are aggregated (input values)
	//    ACC - The type of the accumulator (intermediate aggregate state).
	//    OUT - The type of the aggregated result
    

    // example: https://nightlies.apache.org/flink/flink-docs-release-1.16/api/java/
    public static final class SumAggregateFunction
    implements AggregateFunction<UserBehavior, Integer, Tuple2<String, Integer>> {

		@Override
		public Integer createAccumulator() {
		    return 0; // Initial count
		}
		
		@Override
		public Integer add(UserBehavior value, Integer accumulator) {
		    return accumulator + 1; // Increment count for each UserBehavior
		}
		
		@Override
		public Tuple2<String, Integer> getResult(Integer accumulator) {
		    return new Tuple2<>("category_sum", accumulator); // Return the sum as a Tuple
		}
		
		@Override
		public Integer merge(Integer a, Integer b) {
		    return a + b; // Merge accumulators
		}
	}
}