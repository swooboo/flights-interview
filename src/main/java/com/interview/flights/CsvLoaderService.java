package com.interview.flights;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class CsvLoaderService {

    private final SparkSession sparkSession;

    public Dataset<Row> loadCsv(String path, StructType schema) {

        return sparkSession.read().option("header", "true").schema(schema).csv(path);
    }

    public Dataset<Row> joinFlightsWithAirports(Dataset<Row> flights, Dataset<Row> airports) {
        return flights.join(airports, flights.col("OriginAirportID").equalTo(airports.col("airport_id")), "left")
                .join(airports, flights.col("DestAirportID").equalTo(airports.col("airport_id")), "left");
    }

    public Dataset<Row> normalizeDataFrame(Dataset<Row> df) {
        // Rename each column to follow CapitalCamelCase
        for (String columnName : df.columns()) {
            String newName = convertSnakeCaseToCamelCase(columnName);
            df = df.withColumnRenamed(columnName, newName);
        }
        return df;
    }

    public static String convertSnakeCaseToCamelCase(String input) {
        return Arrays.stream(input.split("_"))
                .map(word -> word.isEmpty() ? word : Character.toUpperCase(word.charAt(0)) + word.substring(1).toLowerCase())
                .collect(Collectors.joining());
    }
}
