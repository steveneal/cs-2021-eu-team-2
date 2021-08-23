package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class TradeSideBiasExtractor implements RfqMetadataExtractor{
    public String since;

    public TradeSideBiasExtractor() {
        this.since = DateTime.now().minusWeeks(1).toString();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String query_buy = String.format("SELECT sum(LastQty) from trade where SecurityID='%s' AND EntityID='%s' AND Side='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                rfq.getEntityId(),
                1,
                since);

        String query_sell = String.format("SELECT sum(LastQty) from trade where SecurityID='%s' AND EntityID='%s' AND Side='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                rfq.getEntityId(),
                1,
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults_buy = session.sql(query_buy);
        Dataset<Row> sqlQueryResults_sell = session.sql(query_sell);

        Object volume_buy = sqlQueryResults_buy.first().get(0);
        if (volume_buy == null) {
            volume_buy = 0L;
        }
        Object volume_sell = sqlQueryResults_sell.first().get(0);
        if (volume_sell == null) {
            volume_sell = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.instrumentTradeSideBias, (Long)volume_buy/(Long)volume_sell);
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
