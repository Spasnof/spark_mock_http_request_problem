package myprojects.spark_sandbox.utl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Encoders}

import scala.io.BufferedSource
import scala.reflect.runtime.universe._


object json_utls {
  def loadUrlOfJsonObjectAndAppend[T <: Product : TypeTag](urlColumn: Column, newColumnName: String, fetchMethod: String => BufferedSource = scala.io.Source.fromURL)(df: DataFrame): DataFrame = {
    val urlToJsonUDF = udf((x: String) => loadFromUrl(x, fetchMethod))

    df.withColumn(
      newColumnName,
      from_json(urlToJsonUDF(urlColumn), Encoders.product[T].schema)
    )
  }

  /**
   * generates a string from a url
   *
   * @param url
   * @param fetchMethod fetchMethod, default is scala.io.Source.fromURL can be overwrittent to mock for unit testing
   * @return a string
   */
  def loadFromUrl(url: String, fetchMethod: String => BufferedSource = scala.io.Source.fromURL): String = {
    val sourceBuffer = fetchMethod(url)
    try {
      sourceBuffer.mkString
    } finally {
      sourceBuffer.close()
    }
  }

}
