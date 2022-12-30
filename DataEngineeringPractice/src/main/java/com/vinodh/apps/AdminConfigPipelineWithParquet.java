package com.vinodh.apps;

import com.vinodh.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class AdminConfigPipelineWithParquet implements SparkApp {

    @Override
    public void execute() {

        final String ADMIN_CONFIG_PATH = SparkUtils.getProperty("admin.parquet.path");
        final String OUTPUT_DIR = SparkUtils.getProperty("results.folder");
        final String BLOCK_SIZE = SparkUtils.getProperty("block.size");
        final SparkSession spark = SparkUtils.getSpark();
        spark.conf().set("spark.app.name", this.getClass().getCanonicalName());

        Dataset<Row> adminDS = spark.read()
                .option("nullValue", "Missing_Data")
                .option("inferSchema", true)
                .option("timestampFormat", "dd/MM/yyyy HH:mm:ss")
                .parquet(ADMIN_CONFIG_PATH);

        adminDS.printSchema();

        adminDS = adminDS
                .withColumn("lastUpdatedAt", functions.col("updatedDate").cast(DataTypes.TimestampType))
                .withColumn("YoYo", functions.lit(null).cast(DataTypes.StringType))
                .withColumnRenamed("defaultCurrencyCode", "currencyCode")
                .where("currencyCode is not null")
                .selectExpr("companyId as companyCode", "currencyCode", "description", "lastUpdatedAt", "YoYo")
                .na().fill("Missing Bro !!");
        adminDS.show();

        adminDS.write()
                .option("compression", "snappy")
                .option("encoding", "BIT_PACKED")
                .option("maxFileSize", BLOCK_SIZE)
                .option("defaultTimestampFormat", "dd/MM/yyyy HH:mm:ss")
                .option("writeLegacyFormat", false)
                .option("enableSummaryMetadata", true)
                .option("outputDataMetrics", true)
                .partitionBy("currencyCode")
                .mode(SaveMode.Overwrite)
                .parquet(OUTPUT_DIR + "/parquet/");

    }
}
