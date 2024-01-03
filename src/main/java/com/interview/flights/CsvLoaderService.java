package com.interview.flights;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;
import static org.apache.spark.sql.functions.col;

@Service
@RequiredArgsConstructor
public class CsvLoaderService {

    private final SparkSession sparkSession;

    public Dataset<Row> loadCsv(String path) {
        return sparkSession.read().option("header", "true").csv(path);
    }

    public Dataset<Row> joinFlightsWithAirports(Dataset<Row> flights, Dataset<Row> airports) {
        return flights.join(airports, flights.col("OriginAirportID").equalTo(airports.col("airport_id")), "left")
                .join(airports, flights.col("DestAirportID").equalTo(airports.col("airport_id")), "left");
    }
}
