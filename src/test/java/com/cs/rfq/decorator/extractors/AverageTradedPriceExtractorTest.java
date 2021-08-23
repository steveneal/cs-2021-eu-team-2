package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AverageTradedPriceExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;

    String since = "2021-08-17";

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-4.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkVolumeWhenAllTradesMatch() {

        AverageTradedPriceExtractor extractor = new AverageTradedPriceExtractor();
        //extractor.setSince(since);

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.instrumentAvgTradePrice);

        assertEquals(135.523875, result);
    }

}
