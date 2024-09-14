import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object IoTTemperatureMonitoring {
  def main(args: Array[String]): Unit = {
    val hadoopHomeDir = "C:\\hadoop"

    import java.nio.file.{Files, Paths}

    if (Files.exists(Paths.get(hadoopHomeDir))) {
      println(s"Path exists: $hadoopHomeDir")
    } else {
      println(s"Path does not exist: $hadoopHomeDir")
    }

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      println("Detected Windows")
      System.setProperty("hadoop.home.dir", hadoopHomeDir)
      println("Hadoop home dir after setting: " + System.getProperty("hadoop.home.dir"))
    }

    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("IoT Temperature Monitoring")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Schema for temperature data
    val temperatureSchema = new StructType()
      .add("device_id", IntegerType)
      .add("temperature", IntegerType)
      .add("timestamp", StringType)

    // Schema for max temperature data
    val maxTempSchema = new StructType()
      .add("device_id", IntegerType)
      .add("max_temp", IntegerType)

    // Read the max temperature data
    val maxTempDF = spark.read
      .option("header", "true")
      .schema(maxTempSchema)
      .csv("C:\\maxTempdata\\maxTempData.csv")

    // Read the temperature data as streaming
    val tempStreamDF = spark.readStream
      .option("header", "true")
      .schema(temperatureSchema)
      .csv("C:\\Shreeraksha_secoundCaseStudy")

    // Convert timestamp to actual timestamp format after trimming whitespace
    val tempDFWithTimestamp = tempStreamDF
      .withColumn("timestamp", trim($"timestamp"))  // Trim whitespace
      .withColumn("timestamp", to_timestamp($"timestamp", "HH:mm:ss"))

    // Group by device_id and 10-minute window to calculate average temperature
    val avgTempDF = tempDFWithTimestamp
      .withWatermark("timestamp", "10 minutes")
      .groupBy($"device_id", window($"timestamp", "10 minutes"))
      .agg(avg($"temperature").as("avg_temperature"))

    // Join with max temperature data
    val joinedDF = avgTempDF.join(maxTempDF, "device_id")

    // Filter where avg temperature exceeds max allowed temperature
    val alertDF = joinedDF
      .filter($"avg_temperature" > $"max_temp")
      .select($"device_id", $"avg_temperature", $"max_temp", $"window.start".as("window_start"), $"window.end".as("window_end"))

    // Output the results to the console
    val query = alertDF.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("10 seconds"))  // Trigger every 10 seconds
      .format("console")
      .start()

    query.awaitTermination()
  }
}
