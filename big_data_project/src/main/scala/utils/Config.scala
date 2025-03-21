package utils

object Config {

  // The local directory containing this repository
  val projectDir :String = "C:/Users/elisa/Desktop/UNI/MAGISTRALE/Big Data"
  // The name of the shared bucket on AWS S3 to read datasets
  val s3sharedBucketName :String = "bd2425-ealbertini"
  // The name of your bucket on AWS S3
  val s3bucketName :String = "bd2425-ealbertini"
  // The path to the credentials file for AWS (if you follow instructions, this should not be updated)
  val credentialsPath :String = "/aws_credentials.txt"

}
