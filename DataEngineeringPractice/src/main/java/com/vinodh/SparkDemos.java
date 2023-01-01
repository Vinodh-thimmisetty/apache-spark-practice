package com.vinodh;


import com.vinodh.apps.AdminConfigPipelineWithParquet;
import com.vinodh.apps.AwsEmrConfigPipelineWithJSON;
import com.vinodh.apps.CustomerPipelineWithSnowflake;
import com.vinodh.apps.EmployeePipelineWithMySQL;
import com.vinodh.apps.HelloVinodh;
import com.vinodh.apps.HelloWorld;
import com.vinodh.apps.MoviesPipelineWithMongoDB;
import com.vinodh.apps.SalesPipelineWithCSV;
import com.vinodh.apps.SparkApp;

public class SparkDemos {

    public static void main(String[] args) {
        SparkApp.setup(new HelloWorld()).execute();
        SparkApp.setup(new HelloVinodh()).execute();
        SparkApp.setup(new SalesPipelineWithCSV()).execute();
        SparkApp.setup(new AwsEmrConfigPipelineWithJSON()).execute();
        SparkApp.setup(new AdminConfigPipelineWithParquet()).execute();
        SparkApp.setup(new EmployeePipelineWithMySQL()).execute();
        SparkApp.setup(new MoviesPipelineWithMongoDB()).execute();

        SparkApp.setup(new CustomerPipelineWithSnowflake()).execute();

    }


}
