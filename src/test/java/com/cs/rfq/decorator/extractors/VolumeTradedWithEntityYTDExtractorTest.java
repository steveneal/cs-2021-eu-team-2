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

public class VolumeTradedWithEntityYTDExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkVolumeWhenPartOfTradesMatch() {

        VolumeTradedWithEntityYTDExtractor extractor = new VolumeTradedWithEntityYTDExtractor();
        extractor.setSince("2018-02-22", "2018-02-01", "2017-03-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();


        results.put(volumeTradedWeekToDate, meta.get(volumeTradedWeekToDate));
        results.put(volumeTradedMonthToDate, meta.get(volumeTradedMonthToDate));
        results.put(volumeTradedYearToDate, meta.get(volumeTradedYearToDate));

        assertEquals(550_000L, results.get(volumeTradedWeekToDate));
        assertEquals(600_000L, results.get(volumeTradedMonthToDate));
        assertEquals(1_000_000L, results.get(volumeTradedYearToDate));
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() {

        //all test trade data are for 2018 so this will cause no matches
        VolumeTradedWithEntityYTDExtractor extractor = new VolumeTradedWithEntityYTDExtractor();
        extractor.setSince("2019-02-22", "2019-02-01", "2019-03-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(volumeTradedYearToDate);

        assertEquals(0L, result);
    }

}
