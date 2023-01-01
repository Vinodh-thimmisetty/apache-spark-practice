package com.vinodh.apps;

import com.vinodh.utils.SparkUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

@Slf4j
public class MoviesPipelineWithMongoDB implements SparkApp {

    @SneakyThrows
    @Override
    public void execute() {
        final String connectionURL = SparkUtils.getProperty("spark.mongodb.read.connection.uri");
        final String databaseName = SparkUtils.getProperty("spark.mongodb.read.database");
        final String collectionName = SparkUtils.getProperty("spark.mongodb.read.collection");
        final String OUTPUT_DIR = SparkUtils.getProperty("results.folder");

        SparkSession spark = SparkUtils.getSpark();
        spark.conf().set("spark.app.name", this.getClass().getCanonicalName());

        Dataset<Row> moviesDS = spark.read()
                .format("mongodb")
                .option("connection.uri", connectionURL)
                .option("database", databaseName)
                .option("collection", collectionName)
                .option("sampleSize", 100)
                .option("aggregation.allowDiskUse", true)
                .load();
        moviesDS.printSchema();

        // Data Cleanup
        moviesDS = moviesDS
                .filter(col("metacritic").isNotNull()
                        .and(col("languages").isNotNull())
                        .and(col("num_mflix_comments").gt(lit(0)))
                        .and(col("rated").isNotNull())
                        .and(col("type").eqNullSafe(lit("movie"))))
                .select("title", "plot", "runtime", "countries", "released", "year", "cast", "imdb", "genres", "cast", "writers", "directors");

        moviesDS.printSchema();

        // Top IMDB Rated Movies(Minimum of 10K votes) in Each Genre per Year with minimum Runtime of 100 Minutes that are released after 2000
        // Flatten the Data before exploring the Data.
        moviesDS = moviesDS
                .withColumn("imdb_rating", functions.col("imdb").getField("rating"))
                .withColumn("imdb_votes", functions.col("imdb").getField("votes"))
                .withColumn("country", functions.explode(col("countries")))
                .withColumn("actor", functions.explode(col("cast")))
                .withColumn("genre", functions.explode(col("genres")))
                .withColumn("writer", functions.explode(col("writers")))
                .withColumn("director", functions.explode(col("directors")));

        moviesDS.show();

        Dataset<Row> topGenreMoviesPerYear = moviesDS.filter(col("year").gt(lit(2000))
                        .and(col("runtime").geq(lit(100)))
                        .and(col("imdb_votes").geq(lit(10_000))))
                .select("title", "year", "imdb_rating", "genre")
                .groupBy("year", "genre", "title").agg(functions.max("imdb_rating").as("Top_Rated_Movie"))
                .orderBy(col("year").desc_nulls_last());

        topGenreMoviesPerYear.show();

        topGenreMoviesPerYear.write()
                .format("parquet")
                .partitionBy("year")
                .option("compression", "snappy")
                .option("encoding", "BIT_PACKED")
                .mode(SaveMode.Overwrite)
                .save(OUTPUT_DIR + "/parquet/");

        // This save not working with Cloud - check for some alternatives in future..
        // topGenreMoviesPerYear.write()
        //         .format("mongodb")
        //         .option("connection.uri", connectionURL)
        //         .option("database", databaseName)
        //         .option("collection", "top_genre_movies")
        //         .option("maxBatchSize", 100)
        //         .option("operationType", "replace")
        //         .partitionBy("year")
        //         .mode(SaveMode.Overwrite)
        //         .save();


    }
}
