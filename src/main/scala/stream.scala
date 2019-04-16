import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructType}
object stream {
  //  Product with highest transaction amount.
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkExample").master("local").getOrCreate()
    import spark.implicits._
    val userSchema = new StructType().add("name", "string").add("age", "integer")
   /* val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema) // Specify schema of the csv files
      .csv("F:\\My_DEV\\Big_Data\\data\\spsark_stream") // Equivalent to format("csv").load("/path/to/directory")
    val query=csvDF.writeStream.format("console").option("truncate","false").start()
    query.awaitTermination()
*/
    val jsonDF = spark
      .readStream
      .json("F:\\My_DEV\\Big_Data\\data\\spark_stream\\json\\first.json")

    jsonDF.show()// Equivalent to format("csv").load("/path/to/directory")
//    csvDf.writeStream.format("console").option("truncate", "false").start()
  }
}
