package com.vinodh.apps;

import com.vinodh.utils.SparkUITimeout;
import com.vinodh.utils.SparkUtils;
import lombok.SneakyThrows;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Paths;
import java.util.stream.Stream;

import static java.lang.String.join;

// Reference: https://spark.apache.org/docs/latest/sql-data-sources.html
public class AwsEmrConfigPipeline implements SparkApp {

    @SneakyThrows
    @SparkUITimeout(timeout = 1)
    @Override
    public void execute() {
        final String AWS_EMR_CONFIG_PATH = SparkUtils.getProperty("aws_emr.json.path");
        final String OUTPUT_DIR = SparkUtils.getProperty("results.folder");
        final SparkSession spark = SparkUtils.getSpark();
        spark.conf().set("spark.app.name", this.getClass().getCanonicalName());

        StructType awsConfigSchema = new StructType()
                .add("AllocationStrategy", DataTypes.StringType, false)
                .add("TargetCapacity", DataTypes.StringType, false)
                .add("TerminateInstancesWithExpiration", DataTypes.BooleanType, false)
                .add("ValidFrom", DataTypes.TimestampType, false)
                .add("ValidUntil", DataTypes.TimestampType, false)
                .add("Type", DataTypes.StringType, false)
                .add("LaunchSpecifications", DataTypes.createArrayType(DataTypes.StringType), false)
                .add("LaunchTemplateConfigs",
                        DataTypes.createArrayType(new StructType()
                                .add("LaunchTemplateSpecification",
                                        new StructType()
                                                .add("LaunchTemplateId", DataTypes.StringType, false)
                                                .add("Version", DataTypes.StringType, false), false)
                                .add("Overrides",
                                        DataTypes.createArrayType(
                                                new StructType()
                                                        .add("InstanceType", DataTypes.StringType, false)
                                                        .add("WeightedCapacity", DataTypes.IntegerType, false)), false)
                        ), false);  // set the address field to not nullable

        Dataset<Row> configDS = spark.read()
                .format("json") // File Format Type
                .option("delimiter", ",")   // For CSV and all it will be Helpful
                .option("header", false)   // First Line is considered as Header
                .option("inferSchema", false)   //  sample some data and define the SCHEMA automatically
                .option("basePath", Paths.get(AWS_EMR_CONFIG_PATH).getParent().toString()) // Folder Path
                .option("inputFileName", "config*.json") // For Regex File Names
                .option("path", AWS_EMR_CONFIG_PATH) // Exact File location - use this option or pass the path in Load()
                .option("inputFiles", join(",", AWS_EMR_CONFIG_PATH, AWS_EMR_CONFIG_PATH)) // For Multiple Files
                .option("nullValue", "") // How to treat NULLS ?
                .option("multiLine", true) // Value per key or column can be spanned across multiple lines
                .option("escape", "\\") // Backslash as Escape Character
                .option("comment", "//") // Any lines Started with // are ignored
                .option("dateFormat", "yyyy-MM-dd")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .option("partitionColumn", "TargetCapacity") // Easier Filtering Column
                .option("lowerBound", 1) // Based on partitionColumn
                .option("upperBound", 100) // Based on partitionColumn
                .option("columnNameOfCorruptRecord", "json_processing_errors")
                .schema(awsConfigSchema)
                .option("mode", "permissive")
                .load()
                .na().fill("MISSING ? NULL ?", new String[]{"LaunchTemplateConfigs"});

        configDS.printSchema();

        Column[] operatingSystems = Stream.of("Linux", "Windows", "MacOS", "Android").map(functions::lit).toArray(Column[]::new);

        configDS = configDS
                .withColumn("LaunchTemplateConfigs", functions.explode(functions.col("LaunchTemplateConfigs")))
                .withColumn("LaunchTemplateOverrides", functions.explode(functions.col("LaunchTemplateConfigs.Overrides")))
                .withColumn("OperatingSystems", functions.array(operatingSystems))
                .withColumn("IsMacSupported", functions.col("OperatingSystems").getItem(2).eqNullSafe("MacOS"))
                .withColumn("IsWindowsSupported", functions.element_at(functions.col("OperatingSystems"), functions.lit(2)).eqNullSafe("Windows"))
                .select("AllocationStrategy", "ValidFrom", "ValidUntil", "IsMacSupported", "IsWindowsSupported",
                        "LaunchTemplateConfigs.LaunchTemplateSpecification.Version", "LaunchTemplateConfigs.LaunchTemplateSpecification.LaunchTemplateId",
                        "LaunchTemplateOverrides.InstanceType", "LaunchTemplateOverrides.WeightedCapacity");

        configDS.where("InstanceType = 'r3.large' ")
                .write()
                .format("json")
                .mode(SaveMode.Overwrite)
                .save(OUTPUT_DIR + "/json/");
    }
}
