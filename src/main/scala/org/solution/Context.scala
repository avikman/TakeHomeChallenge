package org.solution
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("TakeHomeChallenge")
    .master("local[*]")
    .getOrCreate()
}