package com.vinodh.apps;

import com.vinodh.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class EmployeePipelineWithHive implements SparkApp {
    @Override
    public void execute() {

        SparkSession spark = SparkUtils.getHiveSpark();

        StructType employee = new StructType(new StructField[]{
                new StructField("emp_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("emp_name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("emp_dept", DataTypes.StringType, true, Metadata.empty()),
                new StructField("emp_salary", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("emp_location", DataTypes.StringType, true, Metadata.empty()),
                new StructField("emp_hire_date", DataTypes.DateType, true, Metadata.empty()),

        });

        Dataset<Row> input = spark.read().format("jdbc")
                .option("header", "false")
                .option("url", "jdbc:hive2://localhost:10000/default")
                .option("user", "hive")
                .option("password", "")
                .option("driver", "org.apache.hive.jdbc.HiveDriver")
                .option("query", "select * from default.employee")
                .load();

        System.out.println(Arrays.toString(input.columns()));

        input.show();

//        SparkUtils.getHiveSpark().sql("select * from default.employee").show();

    }

    public static void main(String[] args) {
        new EmployeePipelineWithHive().execute();
    }
}
