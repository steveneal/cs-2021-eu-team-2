package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");
        //String path = args[0];
        String path = "src/test/resources/trades/trades.json";


        SparkConf conf = new SparkConf().setAppName("StreamFromSocket");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        SparkSession session = SparkSession
                .builder()
                .appName("Dataset with SQL")
                .getOrCreate();

        RfqProcessor rfqProcessor = new RfqProcessor(session, jssc, path);
        rfqProcessor.startSocketListener();
    }
}