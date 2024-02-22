package org.learningspark.maria

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source

object HelloSpark extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: HelloSpark filename")
      System.exit(1)
    }

    logger.info("Starting Hello Spark")

    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    val surveyDF = loadSurveyDF(spark, args(0))
    surveyDF.show()

    //logger.info("spark.conf=" + spark.conf.getAll.toString())

    //Process your data

    logger.info("Finished Hello Spark")

    spark.stop()

  }

  def loadSurveyDF(spark: SparkSession, dataFile: String) : DataFrame = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(dataFile)
  }

  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf()
    //Set all Spark Configs
    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k,v) => sparkAppConf.set(k.toString,v.toString))

    sparkAppConf
  }

}
