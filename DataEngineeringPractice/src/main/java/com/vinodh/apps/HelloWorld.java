package com.vinodh.apps;

import com.vinodh.utils.SparkUITimeout;
import com.vinodh.utils.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class HelloWorld implements SparkApp {

    @SparkUITimeout(isEnabled = false, timeout = 5)
    @Override
    public void execute() {
        SparkSession spark = SparkUtils.getSpark();
        log.info("Hello Spark --> " + spark.version());
    }

}