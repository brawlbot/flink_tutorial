package wordcount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// env setting
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.ExecutionConfig;


// connector
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;

import org.apache.flink.streaming.api.datastream.DataStream;

// Logger
import java.util.logging.*; 
import java.io.IOException;

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

        
        ExecutionConfig executionConfig = env.getConfig();


        logger.info(executionConfig.getClass().toString());
        // logger.info(executionConfig.configuration.get("execution.attached"));
        logger.info(executionConfig.toString());
        DataStream<String> text;
        

        String filePath = "/tmp/wordcount.txt";
        
//        final FileSource<String> source =
//            FileSource.forRecordStreamFormat(new TextLineInputFormat(), filePath)
//            .build();
//
//        // Initialize the DataStream from the source
//        text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        
        logger.info("Done");
        
    }
}