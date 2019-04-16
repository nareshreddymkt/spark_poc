import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
object sample {
//  Product with highest transaction amount.
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkExample").master("local").getOrCreate()
    import spark.implicits._
    val df = spark.read.option("header","true").csv("F:\\Test\\data\\vikash\\sample.csv")
      var df3=df.withColumn("Transaction_amount", $"Transaction_amount".cast(sql.types.IntegerType))
    val df2=df3.select("Product","Transaction_amount").groupBy(col("Product"))
      .agg(sum($"Transaction_amount").alias("Transaction_amount"))//.sum("Transaction_amount").alias("Transaction_amount")
      .orderBy(desc("Transaction_amount")).limit(1)//.take(1)
    df2.show()

  }

}
