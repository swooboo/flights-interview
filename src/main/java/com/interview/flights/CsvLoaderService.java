package com.interview.flights;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.max;

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

    public Dataset<Row> addLatestFlightDateColumn(Dataset<Row> df) {
        Dataset<Row> daysOfYear = createDaysOfYearDataset();

        Dataset<Row> joined = df.join(daysOfYear,
                df.col("DayofWeek").equalTo(daysOfYear.col("dayOfWeek"))
                        .and(df.col("DayofMonth").equalTo(daysOfYear.col("dayOfMonth")))
        );

        joined = joined.withColumn("LatestFlightDate", max(joined.col("date"))
                .over(Window.partitionBy(df.col("DayofWeek"), df.col("DayofMonth"))));

        return joined.drop(daysOfYear.col("dayOfWeek"))
                .drop(daysOfYear.col("dayOfMonth"))
                .drop(daysOfYear.col("date"));
    }

    private Dataset<Row> createDaysOfYearDataset() {
        Dataset<Long> daysRange = sparkSession.range(1, 366); // for year 2023
        LocalDate baseDate = LocalDate.of(2023, 1, 1);

        Dataset<Date> localDates = daysRange.map(
                (MapFunction<Long, Date>) day -> Date.valueOf(baseDate.plusDays(day - 1)),
                Encoders.DATE()
        );

        return localDates.selectExpr("value as date",
                "dayofweek(value) as dayOfWeek",
                "dayofmonth(value) as dayOfMonth");
    }
}
