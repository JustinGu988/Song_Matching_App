package SongMatch

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

class CSVProcessorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = {
    SparkSession.builder()
      .appName("CSVProcessorTest")
      .master("local[*]")
      .getOrCreate()
  }

  import spark.implicits._

  "inputTitleProc" should "transform input_title into look up key format" in {
    // Sample data
    val data = Seq(
      testTitleData("Jekyll & Hyde"),
      testTitleData("Singing Om"),
      testTitleData("N'importe quel gars"),
      testTitleData("Romeo And Juliet (Love Theme)"),
      testTitleData("Tooth Fairy"),
      //testTitleData("Electric Blues / Old Fashioned Melody")
    )

    // Convert to Spark DS
    val ds: Dataset[testTitleData] = spark.createDataset(data)

    // Expected result
    val expectedData = Seq(
      testExpectedTitleData("Jekyll & Hyde", "JEKYL&HYDE"),
      testExpectedTitleData("Singing Om", "SINGINOM"),  // exclude g ending with ing
      testExpectedTitleData("N'importe quel gars", "NIMPORTEQUELGAR"),
      testExpectedTitleData("Romeo And Juliet (Love Theme)", "ROMEO&JULIET"), // exclude parentheses & keep AND
      testExpectedTitleData("Tooth Fairy", "TOOTHFAIRY"), // keep vowels
      //testExpectedTitleData("Electric Blues / Old Fashioned Melody", "ELECTRICBLUESOLDFASHIONED") // max 15
    )

    val expectedDS: Dataset[testExpectedTitleData] = spark.createDataset(expectedData)

    // Call function
    val result = CSVProcessor.inputTitleProcess(ds)

    // Convert back to case class
    // ScalaTest expects to work with standard Scala/Java objects, instead of Spark DS
    val collectedResult = result.as[testExpectedTitleData].collect()
    val collectedExpected = expectedDS.as[testExpectedTitleData].collect()

    collectedResult should contain theSameElementsAs collectedExpected
  }
}
