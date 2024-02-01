import org.apache.spark.sql.SparkSession
import SongMatch._

object Main {
  def main(args: Array[String]): Unit = {
    val input_file = "src/main/scala/song_files/input_file.csv"
    val lookupKeyDB1 = "src/main/scala/song_files/lookupKeyDB1.csv"
    val SongCodeDB2 = "src/main/scala/song_files/SongCodeDB2.csv"
    val Matched400 = "src/main/scala/song_files/Matched400.csv"
    val outputPath = "src/main/scala/output"

    val spark = SparkSession
      .builder()
      .appName("Scala Spark CSV Processing")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // Read the CSV files
    val inputFile = CSVReader.sparkReadInputFile(spark, input_file)
    val lookUpKey = CSVReader.sparkReadLookUpKey(spark, lookupKeyDB1)
    val songCode = CSVReader.sparkReadSongCode(spark, SongCodeDB2)
    val matched400 = CSVReader.sparkReadMatched(spark, Matched400)

    // Process the CSV files
    val processedDS = CSVProcessor.sparkProcess(inputFile, lookUpKey, songCode, matched400)

    // Write the CSV files
    CSVWriter.sparkWrite(processedDS, outputPath)

    spark.stop()
  }
}
