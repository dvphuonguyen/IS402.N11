import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._


import com.datastax.oss.driver.api.core.uuid.Uuids
import com.datastax.spark.connector._


case class DemoData(screen_name: String, created_at: String, followers: String, location: String, favorite: String, retweet: String, source: String, text: String, statuses: String)

object StreamHandler {
	def main(args: Array[String]): Unit =  {

        val spark = SparkSession
			.builder
			.appName("Stream Handler")
            .config("spark.cassandra.connection.host", "localhost")
			.getOrCreate()

        import spark.implicits._

        val inputDF = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", "localhost:9092")
			.option("subscribe", "demo_twitter_1")
			.load()

        val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]
		

        val expandedDF = rawDF.map(row => row.split(",;,"))
			.map(row => DemoData(
				row(1),
				row(2),
				row(3),
				row(4),
				row(5),
				row(6),
				row(7),
				row(8),
				row(9)
			))

        val makeUUID = udf(() => Uuids.timeBased().toString)

        val summaryWithIDs = expandedDF.withColumn("uuid", makeUUID())
            .withColumnRenamed("screen_name", "screen_name")
			.withColumnRenamed("created_at", "created_at")
			.withColumnRenamed("followers", "followers")
            .withColumnRenamed("location", "location")
			.withColumnRenamed("favorite", "favorite")
			.withColumnRenamed("retweet", "retweet")
			.withColumnRenamed("source", "source")
			.withColumnRenamed("text", "description")
			.withColumnRenamed("statuses", "statuses")

        val query = summaryWithIDs
            .writeStream
            .trigger(Trigger.ProcessingTime("6 seconds"))
			// .format("console")
            .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
				println(s"Writing to Cassandra $batchID")
				batchDF.write
					.cassandraFormat("demo_twitter_7", "twitter") // table, keyspace
					.mode("append")
					.save()
			}
            .outputMode("update")
            .start()

        query.awaitTermination()
    }
}