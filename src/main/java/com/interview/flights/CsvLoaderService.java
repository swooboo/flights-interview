package com.interview.flights;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CsvLoaderService {

    private final SparkSession sparkSession;

    public Dataset<Row> loadCsv(String path) {
        return sparkSession.read().option("header", "true").csv(path);
    }

    public Dataset<Row> normalizeDataFrame(Dataset<Row> df) {
        return df;
    }
}
