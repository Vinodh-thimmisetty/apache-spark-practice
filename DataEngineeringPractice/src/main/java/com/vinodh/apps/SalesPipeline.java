package com.vinodh.apps;

import com.vinodh.utils.SparkUITimeout;
import com.vinodh.utils.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static org.apache.spark.sql.functions.sum;

@Slf4j
public class SalesPipeline implements SparkApp {

    private static final String FIELD_DESC = "description";
    private static final String ID = "id";
    private static final String NAME = "name";
    private static final String AMOUNT = "amount";
    private static final String TOTAL_SALES_AMOUNT = "totalSalesAmount";

    @SparkUITimeout(timeout = 1)
    @Override
    public void execute() {
        final String CSV_FILE_PATH = SparkUtils.getProperty("sales.csv.path");
        final SparkSession spark = SparkUtils.getSpark();
        spark.conf().set("spark.app.name", this.getClass().getCanonicalName());
        final StructType salesSchema = new StructType(Arrays.asList(
                new StructField(ID, DataTypes.IntegerType, false,
                        new MetadataBuilder()
                                .putString(FIELD_DESC, "Unique ID for a Sales Person")
                                .build()),
                new StructField(NAME, DataTypes.StringType, false,
                        new MetadataBuilder()
                                .putString(FIELD_DESC, "Name of a Sales Person")
                                .build()),
                new StructField(AMOUNT, DataTypes.DoubleType, true,
                        new MetadataBuilder()
                                .putString(FIELD_DESC, "Amount of Sales Done by each Sales Person")
                                .build())
        ).toArray(new StructField[0]));


        Dataset<Row> salesDS = spark.read()
                .format("csv")
                .schema(salesSchema)
                .option("header", true)
                .load(CSV_FILE_PATH)
                .na().drop("any", new String[]{ID, NAME});
        salesDS.printSchema();
        salesDS.show(5);
        final WindowSpec salesByName = Window.partitionBy(NAME);
        salesDS = salesDS
                .withColumn(TOTAL_SALES_AMOUNT, sum(AMOUNT).over(salesByName))
                .select(NAME, TOTAL_SALES_AMOUNT)
                .distinct();

        // Top 10 Sales Persons
        salesDS.orderBy(functions.col(TOTAL_SALES_AMOUNT).desc_nulls_last())
                .limit(10)
                .show();

        // Bottom 10 Sales Persons
        salesDS.orderBy(functions.col(TOTAL_SALES_AMOUNT).asc_nulls_last())
                .limit(10)
                .show();

    }
}
