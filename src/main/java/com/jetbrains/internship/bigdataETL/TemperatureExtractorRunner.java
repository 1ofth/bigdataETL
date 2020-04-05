package com.jetbrains.internship.bigdataETL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.jetbrains.internship.bigdataETL.TemperatureExtractor.calculateStatistics;
import static com.jetbrains.internship.bigdataETL.TemperatureExtractor.prepareDS;
import static com.jetbrains.internship.bigdataETL.TemperatureExtractor.saveToDB;

public class TemperatureExtractorRunner {
    private TemperatureExtractorRunner() {
    }

    private static Properties loadProperties() {
        try(InputStream is = TemperatureExtractorRunner.class.getClassLoader().getResourceAsStream("project.properties")) {
            Properties props = new Properties();
            props.load(is);
            return props;
        } catch (IOException e) {
            throw new RuntimeException("Properties file wasn't found!");
        }
    }

    public static void main(final String[] args) {
        Properties props = loadProperties();

        SparkSession session = SparkSession.builder()
                .appName("TemperatureExtractor")
                .getOrCreate();

        Dataset<Row> ds = prepareDS(session, args[0]);

        saveToDB(calculateStatistics(ds), props);

        session.stop();
    }
}
