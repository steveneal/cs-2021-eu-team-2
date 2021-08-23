package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TotalTradesWithEntityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        //rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-3.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }


    @Test
    public void checkNumberOfAllTradesThatMatch() {

        //all test trade data are for 2018 so this will cause no matches
        TotalTradesWithEntityExtractor extractor = new TotalTradesWithEntityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        results.put(tradesWithEntityToday, meta.get(tradesWithEntityToday));
        results.put(tradesWithEntityPastWeek, meta.get(tradesWithEntityPastWeek));
        results.put(tradesWithEntityPastYear, meta.get(tradesWithEntityPastYear));

        assertEquals(0L, results.get(tradesWithEntityToday));
        assertEquals(6L, results.get(tradesWithEntityPastWeek));
        assertEquals(6L, results.get(tradesWithEntityPastYear));
    }

}
