import scala.io.Source
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, trim}
import SongMatch._

object CSVReader {
  /**
   * Read data from csv.
   * - Set tab-delimit
   * - Rename cols
   * - Trim all cells (for key comparison when joining)
   */
  def sparkReadInputFile(spark: SparkSession, path: String): Dataset[inputData] = {
    import spark.implicits._
    val temp = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(path)

    trimAllCells(temp).as[inputData]
  }

  def sparkReadLookUpKey(spark: SparkSession, path: String): Dataset[lookUpData] = {
    import spark.implicits._
    val temp = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(path)
      .withColumnRenamed("database_song_code", "lookup_song_code")
      .withColumnRenamed("database_abb05v_writers", "lookup_writers")

    trimAllCells(temp).as[lookUpData]
  }

  def sparkReadSongCode(spark: SparkSession, path: String): Dataset[songCodeData] = {
    import spark.implicits._
    val temp = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(path)

    trimAllCells(temp).as[songCodeData]
  }

  def sparkReadMatched(spark: SparkSession, path: String): Dataset[matchedData] = {
    import spark.implicits._
    val temp = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(path)
      .withColumnRenamed("unique_input_record_identifier", "unique_input_record_identifier_matched400")
      .withColumnRenamed("input_title", "input_title_matched400")
      .withColumnRenamed("lookup_key", "lookup_key_matched400")
      .withColumnRenamed("input_writers", "input_writers_matched400")
      .withColumnRenamed("input_performers", "input_performers_matched400")
      //.withColumnRenamed("", "database_song_code_matched400")
      .withColumnRenamed("database_song_title", "database_song_title_matched400")
      .withColumnRenamed("database_song_writers", "database_song_writers_matched400")

    // rename the 6th col (empty title)
    val oldColumnName = temp.columns(5)
    val renamed = temp.withColumnRenamed(oldColumnName, "database_song_code_matched400")

    trimAllCells(renamed).as[matchedData]
  }

  def trimAllCells(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (ds, columnName) =>
      ds.withColumn(columnName, trim(col(columnName)))
    }
  }
}
