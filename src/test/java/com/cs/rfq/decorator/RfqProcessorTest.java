package com.cs.rfq.decorator;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.RfqProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RfqProcessorTest {

    private SparkSession session;
    private JavaStreamingContext streamingContext;
    private SparkConf conf;
    private RfqProcessor rfqProcessor;
    private Rfq testRfq;
    private Dataset<Row> trades;


    @BeforeEach
    void setupRfqProcessor() {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");
        conf = new SparkConf().setAppName("StreamFromSocket");
        streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        session = SparkSession
                .builder()
                .appName("Dataset with SQL")
                .getOrCreate();

        String filePath = getClass().getResource("/trades/trades.json").getPath();


        rfqProcessor = new RfqProcessor(session, streamingContext, filePath);

        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        testRfq = Rfq.fromJson(validRfqJson);
    }



    @Test
    void startSocketListener() {
    }

    @Test
    void testProcessRfq() {
        rfqProcessor.processRfq(testRfq);
    }
}