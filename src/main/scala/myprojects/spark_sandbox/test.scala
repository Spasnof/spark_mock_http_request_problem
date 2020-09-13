package myprojects.spark_sandbox

import myprojects.spark_sandbox.utl.json_utls
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

case class MyCaseClass(
                        urlField: String
                      )

case class JsonSchema(
                       userId: Int,
                       id: Int,
                       title: String,
                       completed: Boolean
                     )


object test {
  def plusOne(num: Int) = {
    num + 1
  }

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    val sc = new SparkContext("local[*]", "SparkDemo")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val jsonUrl = "https://jsonplaceholder.typicode.com/todos/1"

    val myCaseClassDf = Seq(MyCaseClass(urlField = jsonUrl)).toDF
    val myCaseClassDfTransformed =
      myCaseClassDf
        .transform(
          json_utls.loadUrlOfJsonObjectAndAppend[JsonSchema]($"urlField", "jsonExpanded")
        )


    myCaseClassDfTransformed.show(false)
    myCaseClassDfTransformed.printSchema()

  }
}