package com.interview.flights;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class SparkSpringApplication implements CommandLineRunner {

    private final CsvLoaderService csvLoaderService;

    public static void main(String[] args) {
        SpringApplication.run(SparkSpringApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Dataset<Row> airportsDf = csvLoaderService.loadCsv("C:\\Users\\user\\IdeaProjects\\flights-interview\\src\\main\\resources\\airports.csv", Schemas.AIRPORTS_SCHEMA);   //TODO: Should be configurable
        Dataset<Row> flightsDf = csvLoaderService.loadCsv("C:\\Users\\user\\IdeaProjects\\flights-interview\\src\\main\\resources\\flights.csv", Schemas.FLIGHTS_SCHEMA);

        Dataset<Row> flightsJoinedDf = csvLoaderService.joinFlightsWithAirports(flightsDf, airportsDf);
        Dataset<Row> flightsWithDatesDf = csvLoaderService.addLatestFlightDateColumn(flightsJoinedDf);
    }
}
