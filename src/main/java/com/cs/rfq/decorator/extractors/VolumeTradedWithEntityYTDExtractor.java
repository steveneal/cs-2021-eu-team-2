package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class VolumeTradedWithEntityYTDExtractor implements RfqMetadataExtractor {

    private String sinceWeek;
    private String sinceMonth;
    private String sinceYear;

    public VolumeTradedWithEntityYTDExtractor() {
        this.sinceWeek = DateTime.now().minusWeeks(1).toString();
        this.sinceMonth = DateTime.now().minusMonths(1).toString();
        this.sinceYear = DateTime.now().minusYears(1).toString();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String queryWeek = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                sinceWeek);

        String queryMonth = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                sinceMonth);

        String queryYear = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                sinceYear);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryWeekResults = session.sql(queryWeek);
        Dataset<Row> sqlQueryMonthResults = session.sql(queryMonth);
        Dataset<Row> sqlQueryYearResults = session.sql(queryYear);

        Object volumeWeek = sqlQueryWeekResults.first().get(0);
        if (volumeWeek == null) {
            volumeWeek = 0L;
        }

        Object volumeMonth = sqlQueryMonthResults.first().get(0);
        if (volumeMonth == null) {
            volumeMonth = 0L;
        }

        Object volumeYear = sqlQueryYearResults.first().get(0);
        if (volumeYear == null) {
            volumeYear = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedYearToDate, volumeYear);
        results.put(RfqMetadataFieldNames.volumeTradedMonthToDate, volumeMonth);
        results.put(RfqMetadataFieldNames.volumeTradedWeekToDate, volumeWeek);

        return results;
    }

    protected void setSince(String sinceWeek, String sinceMonth, String sinceYear) {
        this.sinceWeek = sinceWeek;
        this.sinceMonth = sinceMonth;
        this.sinceYear = sinceYear;
    }
}
