package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.RfqMetadataExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TotalTradesWithEntityExtractor;
import com.cs.rfq.decorator.extractors.VolumeTradedWithEntityYTDExtractor;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext, String filePath) {
        this.session = session;
        this.streamingContext = streamingContext;

        trades = new TradeDataLoader().loadTrades(session, filePath);

        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
    }

    public void startSocketListener() throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        JavaDStream<String> streamInput = streamingContext.socketTextStream("localhost", 9000);

        //TODO: convert each incoming line to a Rfq object and call processRfq method with it

        streamInput.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> processRfq(Rfq.fromJson(line)));
        });

        //TODO: start the streaming context
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        for (RfqMetadataExtractor extractor : extractors){
            metadata.putAll(extractor.extractMetaData(rfq, session, trades));
        }

        publisher.publishMetadata(metadata);
    }
}
