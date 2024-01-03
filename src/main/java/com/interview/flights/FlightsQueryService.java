package com.interview.flights;

import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;

@Service
public class FlightsQueryService {

    @SneakyThrows
    public Dataset<Row> queryTopFlights(Dataset<Row> dataset) {
        Dataset<Row> groupedDataset = dataset.groupBy("LatestFlightDate", "Carrier", "OriginAirportId", "OriginAirportName")
                                             .count();

        WindowSpec windowSpec = Window.orderBy(functions.desc("count"));

        return groupedDataset.withColumn("rank", functions.rank().over(windowSpec))
                             .where(functions.col("rank").leq(20))
                             .drop("rank", "OriginAirportId");
    }

    @SneakyThrows
    public Dataset<Row> queryTopTracks(Dataset<Row> dataset) {
        Dataset<Row> groupedDataset = dataset.groupBy("Carrier", "OriginAirportID", "DestAirportID", "OriginAirportName", "DestAirportName")
                .count();

        return groupedDataset.orderBy(functions.desc("count"))
                .drop("OriginAirportID", "DestAirportID")
                .limit(10);
    }
}
