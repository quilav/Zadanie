import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import scala.util.{Either, Try}

object TransformationJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("TransformationJob")
      .master("local[*]")
      .getOrCreate()

    val deviceSchema = Encoders.product[Device].schema
    val geoNetworkSchema = Encoders.product[GeoNetwork].schema

    val inputPath = "/app/dane.csv"
    val outputPath = "/app/wyniki"

    val result: Either[String, DataFrame] = for {
      rawData <- readData(spark, inputPath, header = true, "\"", "\"")
      processedData <- processJsonColumns(rawData, deviceSchema, geoNetworkSchema)
      explodedData <- extractNestedColumns(processedData)
      preprocessedData <- prepareDataBeforeGrouping(explodedData)
      sortedData <- sortListByDate(preprocessedData)
      limitedData <- limitListToLastVisitors(sortedData, 5)
      resultData <- prepareJsonStructure(limitedData)
      pivotedDF <- pivotAndAggregateData(resultData)
      _ <- writeResultToJson(pivotedDF, outputPath)
    } yield pivotedDF

    result match {
      case Left(error) =>
        logError(s"Error during the transformation process: $error")
      case Right(_) =>
    }

    spark.stop()
  }

  private def readData(spark: SparkSession, filePath: String, header: Boolean, quote: String, escape: String): Either[String, DataFrame] =
    Try {
      spark.read
        .option("header", header)
        .option("quote", quote)
        .option("escape", escape)
        .csv(filePath)
    }.toEither.left.map(e => s"Error occurred during data reading: ${e.getMessage}")

  private def processJsonColumns(rawData: DataFrame, deviceSchema: StructType, geoNetworkSchema: StructType): Either[String, DataFrame] =
    Try {
      rawData
        .withColumn("device", from_json(col("device"), deviceSchema))
        .withColumn("geoNetwork", from_json(col("geoNetwork"), geoNetworkSchema))
    }.toEither.left.map(e => s"Error during JSON column processing: ${e.getMessage}")

  private def extractNestedColumns(processedData: DataFrame): Either[String, DataFrame] =
    Try {
      processedData.select(
        col("_c0"),
        col("date"),
        col("fullVisitorId"),
        col("device.*"),
        col("geoNetwork.*"))
    }.toEither.left.map(e => s"Error during nested columns extraction: ${e.getMessage}")

  private def prepareDataBeforeGrouping(explodedData: DataFrame): Either[String, DataFrame] =
    Try {
      explodedData
        .withColumn("id_and_date", struct(col("fullVisitorId"), col("date")))
        .groupBy("country", "browser")
        .agg(collect_list("id_and_date").as("id_and_date_list"))
    }.toEither.left.map(e => s"Error during data preparation before grouping: ${e.getMessage}")

  private def sortListByDate(preprocessedData: DataFrame): Either[String, DataFrame] =
    Try {
      preprocessedData.withColumn("id_and_date_list", expr("sort_array(id_and_date_list, true)"))
    }.toEither.left.map(e => s"Error during list sorting by date: ${e.getMessage}")

  private def limitListToLastVisitors(sortedData: DataFrame, limit: Int): Either[String, DataFrame] =
    Try {
      sortedData.withColumn("id_and_date_list", expr(s"slice(id_and_date_list, 1, $limit)"))
    }.toEither.left.map(e => s"Error during limiting list to last visitors: ${e.getMessage}")

  private def prepareJsonStructure(limitedData: DataFrame): Either[String, DataFrame] =
    Try {
      limitedData
        .groupBy("country")
        .agg(collect_list(struct("browser", "id_and_date_list")).as("browser_data"))
    }.toEither.left.map(e => s"Error during JSON structure preparation: ${e.getMessage}")

  private def pivotAndAggregateData(resultData: DataFrame): Either[String, DataFrame] =
    Try {
      resultData
        .groupBy()
        .pivot("country")
        .agg(first("browser_data").alias("browser_data"))
        .drop("(not set)")
    }.toEither.left.map(e => s"Error during data pivoting and aggregation: ${e.getMessage}")

  private def writeResultToJson(pivotedDF: DataFrame, outputPath: String): Either[String, Unit] =
    Try {
      pivotedDF.coalesce(1).write.json(outputPath)
    }.toEither.left.map(e => s"Error during writing result to JSON: ${e.getMessage}")

  private def logError(message: String): Unit =
    println(s"Error: $message")

  private case class Device(browser: String, browserVersion: String, browserSize: String,
                            operatingSystem: String, operatingSystemVersion: String, isMobile: Boolean,
                            mobileDeviceBranding: String, mobileDeviceModel: String, mobileInputSelector: String,
                            mobileDeviceInfo: String, mobileDeviceMarketingName: String, flashVersion: String,
                            language: String, screenColors: String, screenResolution: String, deviceCategory: String)

  private case class GeoNetwork(continent: String, subContinent: String, country: String, region: String,
                                metro: String, city: String, cityId: String, networkDomain: String,
                                latitude: String, longitude: String, networkLocation: String)
}
