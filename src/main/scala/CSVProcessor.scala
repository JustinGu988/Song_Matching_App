package SongMatch

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions._

object CSVProcessor {
  /**
   * Data process function that perform the following transformation:
   * - I: Transform input_title into look up key.
   * - II: Join with lookUpKey DS & find best match.
   * - III: Join with songCode DS.
   * - IV: Find best match & remove duplicates.
   */
  def sparkProcess(inputFile: Dataset[inputData], lookUpKey: Dataset[lookUpData], songCode: Dataset[songCodeData], matched400: Dataset[matchedData]): Dataset[_] = {
    import inputFile.sparkSession.implicits._

    // I: Transform "input_title" in "inputFile" into my_lookup_key
    val inputProc = inputTitleProcess(inputFile).as[inputDataProcessed]

    // Add an index column (use to preserve original order)
    val inputTitleProcOrd = inputProc.withColumn("index", monotonically_increasing_id())

    // II: Join processed input data with lookUpKey
    // Fix number error in lookup_key first
    val lookUpKeyFix = fixLookUpKey(lookUpKey)
    val joinedLookUp = inputTitleProcOrd
      .join(lookUpKeyFix, inputTitleProcOrd("my_lookup_key") === lookUpKeyFix("lookup_key"), "left")
      .orderBy("index")

    // III: Join the songCode table
    val joinedSongCode = joinedLookUp
      .join(songCode, joinedLookUp("lookup_song_code") === songCode("database_song_code"), "left")
      .orderBy("index")

    // IV: Find the best match based on the similarity of writer
    val bestMatch = writerFindBestMatch(joinedSongCode).orderBy("index")

    testWithMatched400(bestMatch, matched400)
  }


  /**
   * Transform "input_title" in "inputFile".
   */
  def inputTitleProcess(inputFile: Dataset[_]): Dataset[_] = {
    val myUDF = udf(cleanTitleUDF _)
    val inputTitleProc = {
      inputFile.withColumn("my_lookup_key", trim(myUDF(col("input_title"))))
    }
    inputTitleProc
  }

  /**
   * A helper function to make user defined transformation.
   * Clean data with the following rules:
   * - Convert special chars like Ã©
   * - Regular expression process
   *    - Exclude () and content inside
   *    - Remove char g for word that end with ing; i singing thing => i singin thin
   *    - Remove "the"
   *    - Remove "A ","Il ","Ein", "Un ", "La "...
   *    - Convert "and", "'n'" to &
   *    - Remove end with " les", "s", "ss"...
   *    - Convert " Part " to "PT", "lize" to "lise"
   *    - To upper case
   *    - Replace consecutive duplicate chars, like hello to helo (excluding e i o)
   *
   * - Replaces all non-alphanumeric characters with an empty string (keep &)
   * - SubString process (max 25 chars)
   */
  def cleanTitleUDF(input: String): String = {
    val specialCharProcess = cleanSpecialChar(input)

    val regexProcess = specialCharProcess
      // . : matches any character (except a newline)
      // * : quantifier that means "zero or more of the preceding element"
      // ? : makes quantifier "non-greedy"  (match (foo) (bar) as a whole VS separately)
      // (?s) : DOTALL mode, which makes "." matches any char include newline
      .replaceAll("\\((?s).*?\\)", "").trim
      .replaceAll("\\[(?s).*?]", "").trim
      // \\b : a word boundary (pattern will only match if it appears at start and end of a word)
      // \\w : matches any word character [a-zA-Z0-9_]
      .replaceAll("\\b(\\w*)ing\\b", "$1in")
      // ^ : start of the line
      .replaceAll("The ", "")
      .replaceAll(" the ", " ")
      .replaceAll("^A ", "")
      .replaceAll(" A ", " ")
      .replaceAll(" a ", " ")
      .replaceAll("^Il ", "")
      .replaceAll("^Ein ", "")
      .replaceAll("^Un ", "")
      .replaceAll("La ", "")
      .replaceAll("^El ", "")
      .replaceAll("^Das ", "")
      .replaceAll("St\\. ", "")
      .replaceAll(" and ", "&")
      .replaceAll("And ", "&")
      .replaceAll(" 'n' ", "&")
      .replaceAll(" n' ", "&")
      // $ at end : the end of a line
      .replaceAll(" les$", "")
      .replaceAll("s$", "")
      .replaceAll(" Part ", "PT")
      .replaceAll("lize", "lise")
      // Chang to upper case
      .toUpperCase()
      // () : create a capturing group that can be referred to later regular expression
      // \\1 : a back reference to the first capturing group
      // + : quantifier that means "one or more of the preceding element".
      // $1 : used while replacement, refers to the first capturing group
      .replaceAll("([a-df-hj-np-zA-DF-HJ-NP-Z])\\1+", "$1")

    val nonAlpProcess = regexProcess.replaceAll("[^a-zA-Z0-9&]", "")
    val maxProcess = nonAlpProcess.take(25).trim
    maxProcess
  }

  /**
   * A helper function that converts special char into alphabetic char.
   */
  def cleanSpecialChar(input: String): String = {
    input.replace("ä", "a")
      .replace("Ã ", "a")
      .replace("Ã¤", "a")
      .replace("Ã¡", "a")
      .replace("Ã©", "e")
      .replace("Â©", "e")
      .replace("©", "e")
      .replace("Â«", "e")
      .replace("«", "e")
      .replace("Ã¨", "e")
      .replace("¨", "e")
      .replace("Ä±", "i")
      .replace("Â±", "i")
      .replace("±", "i")
      //.replace("í", "i")
      .replace("Ã\u00AD", "i")
      .replace("Ã¶", "o")
      .replace("¶", "o")
      .replace("Ã³", "o")
      .replace("³", "o")
      .replace("Ã¼", "u")
      .replace("¼", "u")
      .replace("Ã§", "c")
      .replace("§", "c")
      .replace("Ä\u009F", "g")
  }

  /**
   * A helper function used to fix number error in "lookup_key" col, "lookUpKey" table.
   */
  def fixLookUpKey(err: Dataset[_]): Dataset[_] = {
    def fixLookUpKeyUDF(s: String): String = {
      s.replaceAll("03", "PM")
        .replaceAll("04", "NC")
        .replaceAll("05", "RE")
        .replaceAll("06", "OC")
    }

    val myUDF = udf(fixLookUpKeyUDF _)
    err.withColumn("lookup_key", myUDF(col("lookup_key")))
      .withColumn("lookup_writers", myUDF(col("lookup_writers")))
  }

  /**
   * Create a similarity score based on writer to determine the best match.
   * - Process input writer and match with writers in song database, to remove duplication
   * - Use Levenshtein distance to match input title with title in song database, to remove duplication
   */
  def writerFindBestMatch(dupTitles: Dataset[_]): Dataset[_] = {
    val myWriterSimiUDF = udf(writerSimiUDF(_: String, _: String))
    // Add similarity col
    val addScoreDS = dupTitles.withColumn("similarity", myWriterSimiUDF(col("input_writers"), col("database_song_writers")))
    // Remove duplicates
    val maxScoreDS = addScoreDS.groupBy("unique_input_record_identifier").agg(max("similarity").alias("max_similarity"))
      .withColumnRenamed("unique_input_record_identifier", "identifier_max_simi")
    val matchScoreDS = addScoreDS.join(
      maxScoreDS,
      addScoreDS.col("unique_input_record_identifier") === maxScoreDS.col("identifier_max_simi") &&
        addScoreDS.col("similarity") === maxScoreDS.col("max_similarity"))
      .drop("similarity").drop("identifier_max_simi")

    // use Levenshtein distance to match song title
    val addDistDS = matchScoreDS.withColumn("distance", titleLeveDist(col("input_title"), col("database_song_title")))
    val minDistDS = addDistDS.groupBy("unique_input_record_identifier").agg(min("distance").alias("min_distance"))
      .withColumnRenamed("unique_input_record_identifier", "identifier_min_distance")
    val matchDistDS = addDistDS.join(
      minDistDS,
      addDistDS.col("unique_input_record_identifier") === minDistDS.col("identifier_min_distance") &&
        addDistDS.col("distance") === minDistDS.col("min_distance"))
      .drop("distance").drop("identifier_min_distance")

    matchDistDS.dropDuplicates("unique_input_record_identifier")
  }

  /**
   * A helper function to calculate the similarity score.
   * - Process song writer data in song DB, cut into chunks: (DAVIDSON D/AKINS R/FARR T/HAYSLIP B) to (DAVIDSON, AKINS, FARR, HAYSLIP)
   * - Process original writer data in input file: (Aaron Sledge,Ayaamii Sledge) to (AARON, SLEDGE, AYAAMII)
   * - Count how many chunks in songDB writer exist in original writers
   */
  def writerSimiUDF(org: String, wriDB: String): Int = {
    if (org != null && wriDB != null) {
      val songWriChunks = cleanSongDBWriter(wriDB)
      val orgCleaned = cleanOrgWriter(org)

      //println(countSimiScore("JOHN SMITH,ROBINSON JAMES", Set("ROBINSON", "JAMES", "ANNA")))
      val score = countSimiScore(orgCleaned, songWriChunks)
      if (score.isNaN || score == null) 888 else score
    } else 0
  }

  /**
   * A helper function to transform song writers in song database.
   * - Clean special char
   * - Split by "/" "-" and space
   * - Take the chunk that has more than 1 char
   * - (DAVIDSON D/AKINS R/FARR T/HAYSLIP B) to Set(DAVIDSON, AKINS, FARR, HAYSLIP)
   * - (CORSTEN FERRY) to Set(CORSTEN, FERRY)
   */
  def cleanSongDBWriter(wriDB: String): Set[String] = {
    // Scala Set has unique value (no dup) and constant processing time regardless data size
    cleanSpecialChar(wriDB).split("[-/ ]").filter(_.length > 1).toSet
  }

  /**
   * A helper function to transform original song writers in lookup key table.
   * - Change to upper case
   * - //Split by "," "." and space
   * - Remove non-alpha characters and non space, and comma
   * - (Aaron Sledge,Ayaamii Sledge) to (AARON SLEDGE,AYAAMII SLEDGE)
   * - (Julie AubÃÂ©,Katrine NoÃÂ«l,Vivianne Roy) to (JULIE AUBE,KATRINE NOEL,VIVIANNE ROY)
   */
  def cleanOrgWriter(org: String): String = {
    cleanSpecialChar(org).toUpperCase()
      .replaceAll("[^a-zA-Z ,]", "")
    //.split("[,. ]").toSet
  }

  /**
   * A helper function to generate the similarity score.
   * Similarity Score is determined by:
   * - I. The number of contained chunk (substrings) (choose high)
   * - II. The length of chunk (choose high)
   * - III. The number of extra non-matched chunks (choose low)
   * - For each chunk, count the number of occurrence & add length of chunk
   * - (E.g. 30000 means 3 times)
   * - (E.g. 900 means length is 9)
   * - Deduce 1 from 99 for each chunk that are not a substring
   * - (E.g. 98 means 1 chunk is not a substring)
   * - countSimiScore("JOHN SMITH,ROBINSON JAMES", Set("JOHN", "SMITH")) // returns 20999 - 2 substrings, 9 length, 0 extra
   * - countSimiScore("JOHN SMITH,ROBINSON JAMES", Set("ROBINSON", "JAMES")) // returns 21399 - 2 substrings, 13 length, 0 extra
   * - countSimiScore("JOHN SMITH,ROBINSON JAMES", Set("ROBINSON", "JAMES", "ANNA")) // returns 21398 - 2 substrings, 13 length, 1 extra
   */
  def countSimiScore(orgCleaned: String, chunks: Set[String]): Int = {
    val baseScore = 99
    chunks.foldLeft(baseScore) { (score, substring) =>
      if (orgCleaned.contains(substring)) {
        score + 10000 + substring.length * 100
      } else score - 1
    }
  }

  /**
   * A helper function to match song title in DB with input title.
   * Using Levenshtein Distance.
   */
  def titleLeveDist(inputTil: Column, songDBTil: Column): Column = {
    val dist = levenshtein(inputTil, songDBTil)
    when(dist.isNull || isnan(dist), 999).otherwise(dist)
  }


  /**
   * For result testing.
   * Joined with matched400 table and compare match for lookup key transformation and the founded song code.
   */
  def testWithMatched400(myDS: Dataset[_], matched400: Dataset[_]): Dataset[output7Col] = {
    import myDS.sparkSession.implicits._
    val compareDS = myDS.join(matched400, myDS("unique_input_record_identifier") === matched400("unique_input_record_identifier_matched400")
      , "left")
      .orderBy("index")
      .withColumnRenamed("lookup_key_matched400", "lookup_key_given")
      .withColumnRenamed("lookup_key", "lookup_key_in_table")
      .withColumnRenamed("my_lookup_key", "lookup_key")

    compareDS.select("unique_input_record_identifier",
      "input_title",
      "lookup_key",
      "lookup_key_given",
      "database_song_code",
      "database_song_title",
      "database_song_writers")
      .as[output7Col]


    //        def removeHead(s: String): String = {
    //          if (s != null) {
    //            val afterOneRemoved = if (s.startsWith("1")) s.substring(1) else s
    //            afterOneRemoved.replaceFirst("^0*", "")
    //          } else s
    //        }
    //
    //        val myRemUDF = udf(removeHead(_: String))
    //
    //        val cleanMatchedCode = compareDS.withColumn("database_song_code", myRemUDF(col("database_song_code")))
    //
    //        val checkMatchedResult = cleanMatchedCode.withColumn("key_result", when(col("lookup_key") === col("lookup_key_given"), "1").otherwise(""))
    //          .withColumn("code_result", when(col("database_song_code") === col("database_song_code_matched400"), "1").otherwise(""))
    //        checkMatchedResult.show()
    //        checkMatchedResult
  }

}