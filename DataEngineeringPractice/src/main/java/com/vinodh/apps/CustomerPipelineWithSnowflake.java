package com.vinodh.apps;

import com.vinodh.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class CustomerPipelineWithSnowflake implements SparkApp {
    @Override
    public void execute() {

        final String SNOWFLAKE_SOURCE_NAME = SparkUtils.getProperty("snowflake.driver");
        final String sfConnectionURL = SparkUtils.getProperty("snowflake.connection.url");
        final String sfUser = SparkUtils.getProperty("snowflake.username");
        final String sfPwd = SparkUtils.getProperty("snowflake.secret");

        SparkSession spark = SparkUtils.getSpark();
        HashMap<String, String> sfOptionsForRead = new HashMap<>();
        sfOptionsForRead.put("sfurl", sfConnectionURL);
        sfOptionsForRead.put("sfUser", sfUser);
        sfOptionsForRead.put("sfPassword", sfPwd);
        sfOptionsForRead.put("sfWarehouse", "COMPUTE_WH");
        sfOptionsForRead.put("sfDatabase", "SNOWFLAKE_SAMPLE_DATA");
        sfOptionsForRead.put("sfSchema", "TPCH_SF1");
        sfOptionsForRead.put("preactions", "ALTER SESSION SET AUTOCOMMIT = TRUE;ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT = 'GeoJSON';");
        sfOptionsForRead.put("postactions", "ALTER SESSION UNSET AUTOCOMMIT;ALTER SESSION UNSET GEOGRAPHY_OUTPUT_FORMAT;");

        Dataset<Row> regionDS = spark.read()
                .format(SNOWFLAKE_SOURCE_NAME)
                .options(sfOptionsForRead)
                .option("dbtable", "REGION") // use query --> for writing custom SQL query
                .option("partitionColumn", "R_REGIONKEY")
                .option("lowerBound", 1)
                .option("upperBound", 10)
                .option("numPartitions", 1)
                .load();
        regionDS.printSchema();
        regionDS.show();

        Dataset<Row> regionAndNationDS = spark.read()
                .format(SNOWFLAKE_SOURCE_NAME)
                .options(sfOptionsForRead)
                .option("query", "select DISTINCT R.R_NAME, N.N_NAME\n" +
                        "    from NATION N LEFT OUTER JOIN REGION R\n" +
                        "    ON N.N_REGIONKEY = R_REGIONKEY;")
                .option("numPartitions", 1)
                .load();
        regionAndNationDS.printSchema();
        regionAndNationDS.show();

        Dataset<Row> customer52973Info = spark.read()
                .format(SNOWFLAKE_SOURCE_NAME)
                .options(sfOptionsForRead)
                .option("query", "select  C.C_NAME, C_ADDRESS, C.C_PHONE, C.C_MKTSEGMENT, C_ACCTBAL, \n" +
                        "        O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE,\n" +
                        "        L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPMODE,\n" +
                        "        S_NAME, S_ADDRESS,S_PHONE, S_ACCTBAL,\n" +
                        "        PS_AVAILQTY, PS_SUPPLYCOST,\n" +
                        "        P_TYPE, P_RETAILPRICE,\n" +
                        "        N.N_NAME,\n" +
                        "        R.R_NAME \n" +
                        "    from CUSTOMER C\n" +
                        "    LEFT OUTER JOIN ORDERS O \n" +
                        "        ON C.C_CUSTKEY = O.O_CUSTKEY\n" +
                        "    LEFT OUTER JOIN LINEITEM L\n" +
                        "        ON O.O_ORDERKEY = L.L_ORDERKEY\n" +
                        "    LEFT OUTER JOIN SUPPLIER S\n" +
                        "        ON S_SUPPKEY = L_SUPPKEY\n" +
                        "    LEFT OUTER JOIN PARTSUPP PS \n" +
                        "        ON L_SUPPKEY =  PS_SUPPKEY\n" +
                        "    LEFT OUTER JOIN PART P \n" +
                        "        ON PS_PARTKEY = P.P_PARTKEY\n" +
                        "    LEFT OUTER JOIN  NATION N\n" +
                        "        ON C.C_NATIONKEY = N.N_NATIONKEY\n" +
                        "    LEFT OUTER JOIN REGION R\n" +
                        "        ON N.N_REGIONKEY = R_REGIONKEY\n" +
                        "WHERE C_CUSTKEY = 52973;")
                .option("numPartitions", 1)
                .load();
        customer52973Info.printSchema();

        HashMap<String, String> sfOptionsForWrite = new HashMap<>();
        sfOptionsForWrite.put("sfurl", sfConnectionURL);
        sfOptionsForWrite.put("sfUser", sfUser);
        sfOptionsForWrite.put("sfPassword", sfPwd);
        sfOptionsForWrite.put("sfWarehouse", "COMPUTE_WH");
        sfOptionsForWrite.put("sfDatabase", "JOSE");
        sfOptionsForWrite.put("sfSchema", "porto");
        sfOptionsForWrite.put("preactions", "ALTER SESSION SET AUTOCOMMIT = TRUE;ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT = 'GeoJSON';");
        sfOptionsForWrite.put("postactions", "ALTER SESSION UNSET AUTOCOMMIT;ALTER SESSION UNSET GEOGRAPHY_OUTPUT_FORMAT;");
        customer52973Info.write().format(SNOWFLAKE_SOURCE_NAME)
                .options(sfOptionsForWrite)
                .option("dbtable", "customer52973")
                .mode(SaveMode.Overwrite).save();


    }
}
