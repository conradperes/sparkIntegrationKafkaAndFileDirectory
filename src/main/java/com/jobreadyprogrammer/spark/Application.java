package com.jobreadyprogrammer.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class Application extends SparkSessionAbstract{

    public static void main(String[] args)  throws StreamingQueryException {
        SparkSession sparkSession = getSparkSession();
        Dataset<Row> lines = getDatasetSocketStream(sparkSession);
        Dataset<String> words = lines.as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> wordsCounts = words.groupBy("value").count();
        StreamingQuery query = wordsCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        query.awaitTermination();
    }


}
