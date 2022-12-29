package com.vinodh.utils;


import com.vinodh.apps.SparkApp;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Objects;

public class SparkUtils {

    @Getter
    private static final SparkSession spark;
    @Getter
    private static final SparkContext sc;

    @SneakyThrows
    private SparkUtils() {
        throw new IllegalAccessException("No! You can't access me directly boss !!");
    }

    static {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Hello World")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.driver.extraJavaOption", "-Dlog4j.configuration=" + new File(Objects.requireNonNull(SparkUtils.class.getClassLoader().getResource("log4j2.xml")).getPath()).getPath())
                .set("spark.executor.extraJavaOption", "-Dlog4j.configuration=" + new File(Objects.requireNonNull(SparkUtils.class.getClassLoader().getResource("log4j2.xml")).getPath()).getPath())
                .registerKryoClasses(new Class[]{SparkApp.class});
        spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        sc = spark.sparkContext();
    }


}