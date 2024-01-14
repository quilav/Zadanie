import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TransformationJob {
  def main(args: Array[String]): Unit = {
    try {
      // Inicjowanie sesji Spark
      val spark = SparkSession.builder
        .appName("TransformationJob")
        .master("local[*]")
        .getOrCreate()

      // Definicja struktury dla danych w kolumnie "device" (json)
      val deviceSchema = StructType(Seq(
        StructField("browser", StringType, nullable = true),
        StructField("browserVersion", StringType, nullable = true),
        StructField("browserSize", StringType, nullable = true),
        StructField("operatingSystem", StringType, nullable = true),
        StructField("operatingSystemVersion", StringType, nullable = true),
        StructField("isMobile", BooleanType, nullable = true),
        StructField("mobileDeviceBranding", StringType, nullable = true),
        StructField("mobileDeviceModel", StringType, nullable = true),
        StructField("mobileInputSelector", StringType, nullable = true),
        StructField("mobileDeviceInfo", StringType, nullable = true),
        StructField("mobileDeviceMarketingName", StringType, nullable = true),
        StructField("flashVersion", StringType, nullable = true),
        StructField("language", StringType, nullable = true),
        StructField("screenColors", StringType, nullable = true),
        StructField("screenResolution", StringType, nullable = true),
        StructField("deviceCategory", StringType, nullable = true)
      ))

      // Definicja struktury dla danych w kolumnie "geoNetwork" (json)
      val geoNetworkSchema = StructType(Seq(
        StructField("continent", StringType, nullable = true),
        StructField("subContinent", StringType, nullable = true),
        StructField("country", StringType, nullable = true),
        StructField("region", StringType, nullable = true),
        StructField("metro", StringType, nullable = true),
        StructField("city", StringType, nullable = true),
        StructField("cityId", StringType, nullable = true),
        StructField("networkDomain", StringType, nullable = true),
        StructField("latitude", StringType, nullable = true),
        StructField("longitude", StringType, nullable = true),
        StructField("networkLocation", StringType, nullable = true)
      ))

      // Wczytanie danych z pliku CSV
      val csvFilePath = "/app/dane.csv"
      val rawData = spark.read
        .option("header", value = true)
        .option("quote", "\"")
        .option("escape", "\"")
        .csv(csvFilePath)

      // Deserializacja kolumn JSON (device i geoNetwork) do struktur danych
      val processedData = rawData
        .withColumn("device", from_json(col("device"), deviceSchema))
        .withColumn("geoNetwork", from_json(col("geoNetwork"), geoNetworkSchema))

      // Ekstrakcja kolumn zagnieżdżonych
      val explodedData = processedData.select(
        col("_c0"),
        col("date"),
        col("fullVisitorId"),
        col("device.*"),    // Wybór wszystkich kolumn ze struktury 'device'
        col("geoNetwork.*") // Wybór wszystkich kolumn ze struktury 'geoNetwork'
      )

      // Przygotowanie danych przed grupowaniem
      val preprocessedData = explodedData
        .withColumn("id_and_date", struct(col("fullVisitorId"), col("date")))
        .groupBy("country", "browser")
        .agg(collect_list("id_and_date").as("id_and_date_list"))

      // Sortowanie listy według daty
      val sortedData = preprocessedData
        .withColumn("id_and_date_list", expr("sort_array(id_and_date_list, true)"))

      // Ograniczenie listy do maksymalnie 5 ostatnich odwiedzających
      val limitedData = sortedData
        .withColumn("id_and_date_list", expr("slice(id_and_date_list, 1, 5)"))

      // Przygotowanie struktury JSON
      val resultData = limitedData
        .groupBy("country")
        .agg(collect_list(struct("browser", "id_and_date_list")).as("browser_data"))

      // Zapisanie wyniku do pliku JSON
      resultData.coalesce(1).write.json("/app/wyniki.json")

      // Zakończenie sesji Spark
      spark.stop()
    } catch {
      // Obsługa błędów
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
