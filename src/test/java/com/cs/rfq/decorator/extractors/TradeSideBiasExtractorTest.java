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

public class TradeSideBiasExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-5.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkVolumeWhenPartOfTradesMatch() {

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setSince("2021-08-22");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        results.put(instrumentTradeSideBias, meta.get(instrumentTradeSideBias));


        assertEquals(0L, results.get(instrumentTradeSideBias));
    }

}
