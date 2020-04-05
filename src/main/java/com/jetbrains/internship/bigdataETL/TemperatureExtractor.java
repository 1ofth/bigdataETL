package com.jetbrains.internship.bigdataETL;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class TemperatureExtractor {

    private static final int YEARS_IN_CENTURY = 100;

    private TemperatureExtractor() {
    }

    public static Dataset<Row> prepareDS(final SparkSession session, final String path) {
        Dataset<Row> ds = session.read().option("header", true).csv(path);
        //dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude
        ds = ds.withColumnRenamed("dt", "Date");

        ds = ds.withColumn("Date", col("Date").cast(DataTypes.DateType));
        ds = ds.withColumn("avg(Temperature)",
                col("AverageTemperature").cast(DataTypes.DoubleType));

        ds = ds.withColumn("min(Temperature)", col("avg(Temperature)"));
        ds = ds.withColumn("max(Temperature)", col("avg(Temperature)"));

        return ds;
    }

    public static Map<String, Dataset<Row>> calculateStatistics(final Dataset<Row> ds) {
        Map<String, Dataset<Row>> result = new HashMap<>();

        // min max avg by city for each year
        Dataset<Row> cities = groupByAndAgg(ds, col("Country"), col("City"),
                year(col("Date")).as("Year"));
        result.put("t_cities_year", cities);

        // min max avg by country for each year
        Dataset<Row> countries = groupByAndAgg(cities, col("Country"), col("Year"));
        result.put("t_countries_year", countries);

//        min max avg by city for each century
        cities = groupByAndAgg(cities,
                col("Country"), col("City"),
                col("Year").divide(YEARS_IN_CENTURY).cast(DataTypes.IntegerType).as("Century"));
        result.put("t_cities_century", cities);

        // min max avg by country for each century
        countries = groupByAndAgg(countries,
                col("Country"),
                col("Year").divide(YEARS_IN_CENTURY).cast(DataTypes.IntegerType).as("Century"));
        result.put("t_countries_century", countries);

//        min max avg by city total
        cities = groupByAndAgg(cities, col("Country"), col("City"));
        result.put("t_cities", cities);

//        min max avg by country total
        countries = groupByAndAgg(countries, col("Country"));
        result.put("t_countries", countries);
        return result;
    }

    public static void saveToDB(final Map<String, Dataset<Row>> datasets, final Properties props) {
        SaveMode saveMode = SaveMode.valueOf(props.getProperty("saveMode"));
        datasets.forEach((tableName, ds) -> ds.write()
                .mode(saveMode)
                .jdbc(props.getProperty("url"), tableName, props));
    }

    private static Dataset<Row> groupByAndAgg(final Dataset<Row> ds, final Column... cols) {
        return ds.groupBy(cols).agg(
                min("min(Temperature)").as("min(Temperature)"),
                avg("avg(Temperature)").as("avg(Temperature)"),
                max("max(Temperature)").as("max(Temperature)")
        ).cache();
    }

}
