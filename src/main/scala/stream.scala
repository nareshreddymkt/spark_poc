import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructType}
object stream {
  //  Product with highest transaction amount.
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkExample").master("local").getOrCreate()
    import spark.implicits._
    val userSchema = new StructType().add("raw", "string")
    val userSchema2 = new StructType().add("name", "string").add("id", "string")
    val csvDF = spark
      .readStream
      .option("sep", "~")
      .schema(userSchema) // Specify schema of the csv files
      .csv("F:\\My_DEV\\Big_Data\\data\\spark_stream\\json") // Equivalent to format("csv").load("/path/to/directory")

    /*var convertUDF: UserDefinedFunction = udf((line:String) => {
      var res=""
      /*var tmp=line.replaceAll("([{}])","")
      tmp.split(",").foreach(x=>{
        res+=x.split(":")(0)
      })
      print(res)
*/
      return res
    })*/
    var convertUDF= udf((line:String) => line.replaceAll("([{}])","").split(",")(0).split(":")(0)
      +" "+line.replaceAll("([{}])","").split(",")(1).split(":")(0))

    val df1=csvDF.withColumn("first",convertUDF(col("raw")))
    /*convertUDF= udf((line:String) => {
      var res=""
  /*    var tmp=line.replaceAll("([{}])","")
      tmp.split(",").foreach(x=>{
        res+=x.split(":")(1)
      })
  */    return res
    })*/
    convertUDF= udf((line:String) => line.replaceAll("([{}])","").split(",")(0).split(":")(1)
      +" "+line.replaceAll("([{}])","").split(",")(1).split(":")(1))

    val df2=df1.withColumn("second",convertUDF(col("raw")))
    val query=df2.writeStream.format("console").option("truncate","false").start()
    query.awaitTermination()
   /* val jsonDF = spark
      .readStream.schema(userSchema2)
//      .json("F:\\My_DEV\\Big_Data\\data\\spark_stream\\json\\first.json")
      .format("json").load("F:\\My_DEV\\Big_Data\\data\\spark_stream\\json")
    val query=jsonDF.writeStream.format("console").option("truncate","false").start()
    query.awaitTermination()*/
  }
}
