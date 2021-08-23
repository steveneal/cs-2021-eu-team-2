package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class TradeSideBiasExtractor {
    public String since;

    public TradeSideBiasExtractor() {
        this.since = DateTime.now().minusWeeks(1).toString();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String query = String.format("SELECT sum(LastQty) from trade where SecurityID='%s' AND EntityID='%s' AND Side='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                rfq.getEntityId(),
                rfq.getSide(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.instrumentAvgTradePrice, volume);
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
