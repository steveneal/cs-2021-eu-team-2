package com.cs.rfq.decorator;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files
        Metadata metaEmpty = new Metadata().empty();

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("TraderId", LongType, false, metaEmpty),
                        new StructField("EntityId", LongType, false, metaEmpty),
                        new StructField("SecurityID", StringType, false, metaEmpty),
                        new StructField("LastQty", LongType, false, metaEmpty),
                        new StructField("LastPx", DoubleType, false, metaEmpty),
                        new StructField("TradeDate", DateType, false, metaEmpty),
                        new StructField("Currency", StringType, false, metaEmpty)
                }
        );

        //TODO: load the trades dataset

        //TODO: log a message indicating number of records loaded and the schema used

        return trades;
    }

}
