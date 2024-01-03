package com.interview.flights;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import java.sql.Date;

@Service
@RequiredArgsConstructor
public class CsvLoaderService {

    private final SparkSession sparkSession;

    public Dataset<Row> loadCsv(String path, StructType schema) {

        return sparkSession.read().option("header", "true").schema(schema).csv(path);
    }

    public Dataset<Row> joinFlightsWithAirports(Dataset<Row> flights, Dataset<Row> airports) {
        return flights.join(airports, flights.col("OriginAirportID").equalTo(airports.col("airport_id")), "left")
                .drop("airport_id")
                .withColumnRenamed("name", "OriginAirportName")
                .join(airports, flights.col("DestAirportID").equalTo(airports.col("airport_id")), "left")
                .withColumnRenamed("name", "DestAirportName");
    }

    public Dataset<Row> addLatestFlightDateColumn(Dataset<Row> df) {    //TODO: Can be done without UDFs by using date range and join
        UDF2<Integer, Integer, Date> getLatestDateUdf = DateUtils::getLatestDate;
        sparkSession.udf().register("getLatestDate", getLatestDateUdf, DataTypes.DateType);
        return df.withColumn("LatestFlightDate", functions.callUDF("getLatestDate", df.col("DayofMonth"), df.col("DayofWeek")));
    }
}
