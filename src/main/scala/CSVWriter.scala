import java.io.{BufferedWriter, FileWriter}
import org.apache.spark.sql.Dataset

object CSVWriter {
  def sparkWrite[T](ds: Dataset[T], path: String): Unit = {
    ds.coalesce(1) // output one csv file when data is small
      .write
      .mode("overwrite") // overwrite existing file
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(path)
  }

}
