package projectJobs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Rdd {

  /**
   * This method creates an RDD with rows [asin, reviewText, overall, category, summary]
   * given the path of a csv file
   * */
  def create(path: String, spark: SparkSession):  RDD[(String, String, Double, String, String)] = {
    spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ",")
      .option("multiline", "true")
      .option("escape", "\"")
      .csv(path).rdd
      .map(row => {
        val asin = row.getAs[String]("asin")
        val reviewText = row.getAs[String]("reviewText")
        val overall = try {
          row.getAs[String]("overall").toDouble
        } catch {
          case e: Exception => 0.0
        }
        val category = row.getAs[String]("category")
        val summary = row.getAs[String]("summary")

        (asin, reviewText, overall, category, summary)
      })}

  /**
   * This method cleans a string substituting all the special characters except from ' and
   * the multiple blank spaces with a blank space. It also trims the string.
   * */
  def cleanString(s: String): String = {
    s.toLowerCase()
      .replaceAll("[^a-zA-z0-9 ']", " ")
      .replaceAll("\\[", " ")
      .replaceAll("\\]", " ")
      .replaceAll("\\s+", " ")
      .trim()
  }
}
