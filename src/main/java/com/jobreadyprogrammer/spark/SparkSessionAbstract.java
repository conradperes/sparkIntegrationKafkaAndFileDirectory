package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionAbstract {
    protected static Dataset<Row> getDatasetInferingSchema(SparkSession spark, String fileType, String fileName) {
        return spark.read().format(fileType)
                .option("inferSchema", "true")
                .option("header", true)
                .load(fileName);
    }

    protected static Dataset<Row> getDatasetSocketStream(SparkSession sparkSession) {
        return sparkSession.readStream()
                .format("socket")
                .option("host","localhost")
                .option("port",9999)
                .load();
    }

    protected static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("Streaming Socket wordcount")
                .master("local")
                .getOrCreate();
    }
}
