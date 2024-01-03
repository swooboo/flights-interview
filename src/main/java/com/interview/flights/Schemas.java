package com.interview.flights;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

public class Schemas {
    public static final StructType AIRPORTS_SCHEMA = new StructType(new StructField[]{
        DataTypes.createStructField("airport_id", DataTypes.IntegerType, false),
        DataTypes.createStructField("city", DataTypes.StringType, true),
        DataTypes.createStructField("state", DataTypes.StringType, true),
        DataTypes.createStructField("name", DataTypes.StringType, true)
    });

    public static final StructType FLIGHTS_SCHEMA = new StructType(new StructField[]{
        DataTypes.createStructField("DayofMonth", DataTypes.IntegerType, true),
        DataTypes.createStructField("DayOfWeek", DataTypes.IntegerType, true),
        DataTypes.createStructField("Carrier", DataTypes.StringType, true),
        DataTypes.createStructField("OriginAirportID", DataTypes.IntegerType, true),
        DataTypes.createStructField("DestAirportID", DataTypes.IntegerType, true),
        DataTypes.createStructField("DepDelay", DataTypes.IntegerType, true),
        DataTypes.createStructField("ArrDelay", DataTypes.IntegerType, true)
    });
}
