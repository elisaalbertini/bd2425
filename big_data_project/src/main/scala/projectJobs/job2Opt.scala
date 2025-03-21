package projectJobs

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import utils._

object job2Opt {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    val sqlContext = sparkSession.sqlContext // needed to save as CSV
    import sqlContext.implicits._

    sparkSession.sparkContext.getPersistentRDDs.foreach(_._2.unpersist())

    val rddReviewElectronics = Rdd.create("s3a://" + Config.s3bucketName + "/datasets/Electronics_5_part0.csv", sparkSession)
    val rddReviewBooks = Rdd.create("s3a://" + Config.s3bucketName + "/datasets/Books_5_part0.csv", sparkSession)
    val rddReviewBooks1 = Rdd.create("s3a://" + Config.s3bucketName + "/datasets/Books_5_part1.csv", sparkSession)

    val p = new HashPartitioner(36)

    // union of the three Rdds
    val rddUnion = rddReviewElectronics
      .union(rddReviewBooks)
      .union(rddReviewBooks1)
      // remove review and category column, clean summary string
      .map({case (id, review, rating, category, summary) => (id, rating, Rdd.cleanString(summary))})
      // remove rows where the cleaned string is empty
      .filter(x => x._3 != "")
      // map id as key, replace summary with the words
      .map(x => (x._1, (x._2, x._3.split(" "))))

    // create filter based on rating and number of reviews
    val filter = rddUnion
      // remove words column
      .map({case (id, (rating, words)) => (id, rating)})
      // aggregate by id to compute the sum of all the ratings for the product and count the number of reviews
      .aggregateByKey((0.0, 0.0))((a, r) => (a._1 + r, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      // compute the average rating, keep the number of reviews
      .map({case (id, (ratingSum, ratingNumber)) => (id, (ratingSum/ratingNumber, ratingNumber))})
      // filter to keep products with high avg rating but few reviews
      .filter(x => x._2._1 >= 4 && x._2._2 < 10)

    val filteredRdd = rddUnion
      // remove rating column
      .map({case (id, (rating, words)) => (id, words)})
      // optimization
      .partitionBy(p)
      // join used to filter the original Rdd
      .join(filter)
      // optimization
      .persist(MEMORY_AND_DISK)

    // now we work with the filter Rdd to count how many times each single word appears in all the summaries
    val wordFreq = filteredRdd
      // flat map is used to create a Rdd where each tuple consist of a word and the number 1
      .flatMap({case (_, (summary, _))=>
        summary
          .map(x=>(x, 1.0))})
      // now it is possible to count the occurrences of each word
      .reduceByKey(_+_)

    // now we work with the filter Rdd to count how many times each single word appears in the summaries of a single product
    val wordFreqPerReview = filteredRdd
      .flatMap({case (id, (summary, _))=>
        summary
          .map(x=>((id, x), 1.0))})
      .reduceByKey(_ + _)
      // map because we need the words as key for the join
      .map({case ((id, word), count) => (word, (id, count))})
      // join to obtain the total frequency of each word
      .join(wordFreq)
      // compute the ratio between the frequency of the word in the reviews of a product and the total frequency
      .map({case (word, ((id, count), tot)) => (word, (id, count/tot))})
      // we use mapValues to place the values inside a List, we do this so that, later, it is possible to
      // put together a list of all the products the word appears in, together with the ratio computed in the previous map
      .mapValues(x => List(x))
      // we create the list appending the values for the same word (key)
      .reduceByKey((a, b) => a ++ b)
      // map to change the key to the number of products a word appears in, and sort based on it
      .map(x => (x._2.size, (x._1, x._2)))
      .sortByKey(ascending = false)
      // we keep only the words that appear in multiple products
      .filter(x => x._1 > 100)
      // map to write as DF on file
      .map(x => (x._1, x._2._1, x._2._2.toString()))

    wordFreqPerReview
      .coalesce(1)
      .toDF()
      .write.format("csv").mode(SaveMode.Overwrite)
      .save("s3a://" + Config.s3bucketName + "/output")
  }
}
