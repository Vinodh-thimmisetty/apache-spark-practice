package com.vinodh.apps;

import com.vinodh.domain.Employee;
import com.vinodh.domain.Sensor;
import com.vinodh.utils.SparkUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Properties;

public class EmployeePipelineWithMySQL implements SparkApp {
    private static final String REPORT_TIMESTAMP = "reportTs";


    @Override
    public void execute() {
        final String connectionURL = SparkUtils.getProperty("mysql.connection.url");
        final String mysqlDriver = SparkUtils.getProperty("mysql.driver");
        final String mysqlUser = SparkUtils.getProperty("mysql.username");
        final String mysqlPwd = SparkUtils.getProperty("mysql.secret");
        final String employees_table = "employees";


        SparkSession spark = SparkUtils.getSpark();
        spark.conf().set("spark.app.name", this.getClass().getCanonicalName());


        // Set connection properties
        Properties mysqlProps = new Properties();
        mysqlProps.put("user", mysqlUser);
        mysqlProps.put("password", mysqlPwd);
        Dataset<Row> empDS = spark.read()
                .option("partitionColumn", "emp_id")
                .option("lowerBound", 1)
                .option("upperBound", 1_000_000)
                .option("numPartitions", 1)
                .option("fetchsize", 10_000)
                .option("maxRows", 1000)
                .jdbc(connectionURL, employees_table, mysqlProps);

        Dataset<Employee> employeesDS = empDS
                .map((MapFunction<Row, Employee>) row ->
                                new Employee(row.getAs("emp_id"),
                                        row.getAs("name"),
                                        row.getAs("position"),
                                        row.getAs("salary")),
                        Encoders.bean(Employee.class));

        employeesDS.printSchema();
        employeesDS.show();

        // Set connection properties
        final String sensorSqlQuery = "SELECT distinct * FROM sensor WHERE reportValue > 0";

        Dataset<Row> sDS = spark.read()
                .format("jdbc")
                .option("url", connectionURL)
                .option("dbtable", "( " + sensorSqlQuery + " ) t") // we can pass table or SQL Query on that table
                .option("user", mysqlUser)
                .option("password", mysqlPwd)
                .option("driver", mysqlDriver)
                .option("columnName", REPORT_TIMESTAMP)
                .option("columnType", "timestamp")
                .option("isolationLevel", "SERIALIZABLE")
                .load();
        sDS.printSchema();

        Dataset<Sensor> sensorDS = sDS
                // Make sure Date/Time Datatype is from java.sql package
                .withColumn(REPORT_TIMESTAMP, functions.to_date(functions.col(REPORT_TIMESTAMP)))
                .as(Encoders.bean(Sensor.class));

        Dataset<Sensor> cachedSensorDS = sensorDS.cache();
        cachedSensorDS.printSchema();
        cachedSensorDS.show();

        cachedSensorDS
                // any change in data-type will be problem while writing to Table.
                .withColumn(REPORT_TIMESTAMP, functions.to_timestamp(functions.col(REPORT_TIMESTAMP)))
                .write()
                .format("jdbc")
                .option("url", connectionURL)
                .option("dbtable", "sensor")
                .option("user", mysqlUser)
                .option("password", mysqlPwd)
                .option("driver", mysqlDriver)
                .option("batchsize", 1000)
                .option("numPartitions", 1)
                .option("isolationLevel", "SERIALIZABLE")
                .option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
                .option("truncate", false)
                .option("connectionProperties", "allowMultiQueries=false;useUnicode=true;characterEncoding=utf8")
                .mode(SaveMode.Overwrite)
                .save();

    }
}
