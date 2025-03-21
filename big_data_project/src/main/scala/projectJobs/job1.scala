package projectJobs

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import utils._

object job1 {

  private def classifyRating(rating: Double): String = {
    rating match {
      case r if r <= 2 => "low rating"
      case r if r == 3 => "medium rating"
      case _ => "high rating"
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    val sqlContext = sparkSession.sqlContext // needed to save as CSV
    import sqlContext.implicits._

    sparkSession.sparkContext.getPersistentRDDs.foreach(_._2.unpersist())

    // create Rdd from csv files
    val rddReviewElectronics = Rdd.create("s3a://" + Config.s3bucketName + "/datasets/Electronics_5_part0.csv", sparkSession)
    val rddReviewBooks = Rdd.create("s3a://" + Config.s3bucketName + "/datasets/Books_5_part0.csv", sparkSession)
    val rddReviewBooks1 = Rdd.create("s3a://" + Config.s3bucketName + "/datasets/Books_5_part1.csv", sparkSession)

    // union of the three Rdds
    val rddUnion = rddReviewElectronics
      .union(rddReviewBooks)
      .union(rddReviewBooks1)

    val categoryRatingReviewWords =
      rddUnion
        // map category as key, remove summary and id, clean review string and replace rating with class of rating
        .map({case (id, review, rating, category, summary) => (category, (Rdd.cleanString(review), classifyRating(rating)))})
        // remove rows where the cleaned string is empty
        .filter(x =>  x._2._1 != "")
        // replace review with the number of words in it
        .map({case (category, (review, rating)) => (category, (rating, review.split(" ").length))})

    val totAllWordPerCategory =
      categoryRatingReviewWords
        // map category as key and the number of words as value while keeping the partitioning
        .mapValues(x => x._2)
        // compute the total number of words for each category, adding the values
        .aggregateByKey(0.0)((a, l) => a + l, (a1, a2) => a1 + a2)

    // first we join these two Rdds to add the total number of words in each category
    val wordFreqSubCategory =
      categoryRatingReviewWords
      .join(totAllWordPerCategory)
      // adding category, rating class and the total number of words per category to the key.
      // We need this in order to compute the number of words for each different key while keeping the number of words per category.
      .map({case (category, ((classification, words), allWords)) => ((category, classification, allWords), words)})
      .reduceByKey(_ + _)
      // map to compute the ratio between the number of words for each class of rating and the total number of words for each category
      // map to write as DF on file
      .map({case ((category, classification, allWords), words) =>
        (category, classification, words/allWords)})

    wordFreqSubCategory
      .coalesce(1)
      .toDF()
      .write.format("csv").mode(SaveMode.Overwrite).
       save("s3a://" + Config.s3bucketName + "/output")
  }
}
