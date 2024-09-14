import org.apache.spark.sql.SparkSession

object StreamingApp {
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

    val spark = SparkSession.builder
      .appName("StreamingApp")
      .master("local[*]")
      .getOrCreate()

    // Set the correct shuffle partitions configuration
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.sparkContext.setLogLevel("WARN")

    // Define the streaming source
    val streamingFiles = spark.readStream.text("C:\\Documents\\Spark-Streaming")

    // Define the query to write to the console
    val query = streamingFiles.writeStream
      .outputMode("append")
      .format("console")
      .start()

    // Wait for the termination of the query
    query.awaitTermination()
  }
}
