package com.github.mrpowers.spark.spec.sql

import java.sql.{Date, Timestamp}

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.github.mrpowers.spark.models._
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest._

class FunctionsSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DatasetComparer {

  import spark.implicits._

  describe("#abs") {

    it("calculates the absolute value") {

      val sourceDF = spark.createDF(
        List(
          (1),
          (-8),
          (-5),
          (null)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("num1abs", abs(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (1, 1),
          (-8, 8),
          (-5, 5),
          (null, null)
        ), List(
          ("num1", IntegerType, true),
          ("num1abs", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#acos") {

    it("calculates the cosine inverse of the given value") {

      val sourceDF = spark.createDF(
        List(
          (-1.0),
          (-0.5),
          (0.5),
          (1.0)
        ), List(
          ("num1", DoubleType, false)
        )
      )

      val actualDF = sourceDF.withColumn("acos_value", acos(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (-1.0, 3.141592653589793),
          (-0.5, 2.0943951023931957),
          (0.5, 1.0471975511965979),
          (1.0, 0.0)
        ), List(
          ("num1", DoubleType, false),
          ("acos_value", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#add_months") {

    it("returns the date that is numMonths after startDate") {

      val sourceDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-01-01 00:00:00")),
          ("2", Timestamp.valueOf("2016-12-01 00:00:00"))
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "future_date",
        add_months(col("birth_date"), 2)
      )

      val expectedDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-01-01 00:00:00"), Date.valueOf("2016-03-01")),
          ("2", Timestamp.valueOf("2016-12-01 00:00:00"), Date.valueOf("2017-02-01"))
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true),
          ("future_date", DateType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#approx_count_distinct") {
    pending
  }

  describe("#array_contains") {
    pending
  }

  describe("#array") {
    pending
  }

  describe("#asc_nulls_first") {

    it("sorts a DataFrame with null values first") {

      val sourceDF = spark.createDF(
        List(null, 1, -8, null, -5),
        List(("num1", IntegerType, true))
      )

      val actualDF = sourceDF.sort(asc_nulls_first("num1"))

      val expectedDF = spark.createDF(
        List(null, null, -8, -5, 1),
        List(("num1", IntegerType, true))
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#asc_nulls_last") {

    it("sorts a DataFrame with null values last") {

      val sourceDF = spark.createDF(
        List(
          (null),
          (1),
          (-8),
          (null),
          (-5)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.sort(asc_nulls_last("num1"))

      val expectedDF = spark.createDF(
        List(
          (-8),
          (-5),
          (1),
          (null),
          (null)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#asc") {

    it("sorts a DataFrame in ascending order") {

      val sourceDF = spark.createDF(
        List(
          (1),
          (-8),
          (-5)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.sort(asc("num1"))

      val expectedDF = spark.createDF(
        List(
          (-8),
          (-5),
          (1)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#ascii") {

    it("Computes the numeric value of the first character of the string column, and returns the result as an int column") {

      val sourceDF = spark.createDF(
        List(
          ("A"),
          ("AB"),
          ("B"),
          ("C"),
          ("1"),
          ("2"),
          ("3")
        ), List(
          ("Chr", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("ASCII", ascii(col("Chr")))

      val expectedDF = spark.createDF(
        List(
          ("A", 65),
          ("AB", 65),
          ("B", 66),
          ("C", 67),
          ("1", 49),
          ("2", 50),
          ("3", 51)
        ), List(
          ("Chr", StringType, true),
          ("ASCII", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#asin") {

    it("Computes the sine inverse of the given column; the returned angle is in the range -pi/2 through pi/2.") {

      val sourceDF = spark.createDF(
        List(
          (1.0),
          (0.2),
          (0.5)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("asin", asin("num1"))

      val expectedDF = spark.createDF(
        List(
          (1.0, 1.5707963267948966),
          (0.2, 0.2013579207903308),
          (0.5, 0.5235987755982989)
        ), List(
          ("num1", DoubleType, true),
          ("asin", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#atan") {

    it("Computes the tangent inverse of the given column") {

      val sourceDF = spark.createDF(
        List(
          (1),
          (5),
          (8)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("atan", atan("num1"))

      val expectedDF = spark.createDF(
        List(
          (1, 0.7853981633974483),
          (5, 1.373400766945016),
          (8, 1.446441332248135)
        ), List(
          ("num1", IntegerType, true),
          ("atan", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#atan2") {
    pending
  }

  describe("#avg") {

    it("Computes the average(an average is the sum of a list of numbers divided by the number of numbers in the list) of a column skipping the null values") {

      val sourceDF = spark.createDF(
        List(
          (7.793357934),
          (167.7902098),
          (-26.26209048),
          (113.8381503),
          (18.01957295),
          (-7.266169154),
          (10.20120724),
          (-658.5405405),
          (-6.702617801),
          (35.99217221),
          (0.0),
          (null)
        ), List(
          ("Double", DoubleType, true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (-31.37606795463637)
        ), List(
          ("average", DoubleType, true)
        )
      )

      val actualDF = sourceDF.agg(avg("Double").as("average"))

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

  }

  describe("#base64") {

    it("Computes the BASE64 encoding of a binary column and returns it as a string column") {

      val sourceDF = spark.createDF(
        List(
          ("01010100")
        ), List(
          ("bin1", StringType, true)
        )
      ).withColumn("bin1", col("bin1").cast("binary"))

      val actualDF = sourceDF.withColumn("base64", base64(col("bin1")))

      val expectedDF = spark.createDF(
        List(
          ("01010100", "MDEwMTAxMDA=")
        ), List(
          ("bin1", StringType, true),
          ("base64", StringType, true)
        )
      ).withColumn("bin1", col("bin1").cast("binary"))

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#bin") {

    it("returns the string representation of the binary value of the given long column") {

      val sourceDF = spark.createDF(
        List(
          (12L),
          (null)
        ), List(
          ("num1", LongType, true)
        )
      )

      val actualDF = sourceDF.withColumn("bin", bin(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (12L, "1100"),
          (null, null)
        ), List(
          ("num1", LongType, true),
          ("bin", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

  }

  describe("#bitwiseNOT") {

    it("Computes bitwise NOT") {

      val sourceDF = spark.createDF(
        List(
          (56),
          (39),
          (-99)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("bitwiseNOT", bitwiseNOT(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (56, -57),
          (39, -40),
          (-99, 98)
        ), List(
          ("num1", IntegerType, true),
          ("bitwiseNOT", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#broadcast") {
    pending
  }

  describe("#bround") {

    it("returns the value of the column e rounded to 0 decimal places with HALF_EVEN round mode") {

      val sourceDF = spark.createDF(
        List(
          (8.1),
          (64.8),
          (3.5),
          (-27.0),
          (null)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("brounded_num", bround(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (8.1, 8.0),
          (64.8, 65.0),
          (3.5, 4.0),
          (-27.0, -27.0),
          (null, null)
        ), List(
          ("num1", DoubleType, true),
          ("brounded_num", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    pending
  }

  describe("#callUDF") {
    pending
  }

  describe("#cbrt") {

    it("computes the cube-root of the given value") {

      val sourceDF = spark.createDF(
        List(
          (8),
          (64),
          (-27)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("cube_root", cbrt(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (8, 2.0),
          (64, 4.0),
          (-27, -3.0)
        ), List(
          ("num1", IntegerType, true),
          ("cube_root", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#ceil") {

    it("rounds the number up to the nearest integer") {

      val numbersDF = spark.createDF(
        List(
          (1.5),
          (-8.1),
          (5.9)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = numbersDF.withColumn("upper", ceil(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (1.5, 2L),
          (-8.1, -8L),
          (5.9, 6L)
        ), List(
          ("num1", DoubleType, true),
          ("upper", LongType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#coalesce") {

    it("returns the first column that is not null, or null if all inputs are null.") {

      val wordsDF = spark.createDF(
        List(
          ("banh", "mi"),
          ("pho", "ga"),
          (null, "cheese"),
          ("pizza", null),
          (null, null)
        ), List(
          ("word1", StringType, true),
          ("word2", StringType, true)
        )
      )
      val actualDF = wordsDF.withColumn(
        "yummy",
        coalesce(
          col("word1"),
          col("word2")
        )
      )

      val expectedDF = spark.createDF(
        List(
          ("banh", "mi", "banh"),
          ("pho", "ga", "pho"),
          (null, "cheese", "cheese"),
          ("pizza", null, "pizza"),
          (null, null, null)
        ), List(
          ("word1", StringType, true),
          ("word2", StringType, true),
          ("yummy", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#col") {

    it("Returns a Column based on the given column name.") {

      val sourceDF = spark.createDF(
        List(
          (1),
          (4),
          (99)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("col", col("num1"))

      val expectedDF = spark.createDF(
        List(
          (1, 1),
          (4, 4),
          (99, 99)
        ), List(
          ("num1", IntegerType, true),
          ("col", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#collect_list") {

    it("Aggregate function: returns a list of objects with duplicates") {

      val sourceDF = spark.createDF(
        List(
          ("A", "cat"),
          ("B", "cat"),
          ("A", "bat"),
          ("C", "matt"),
          ("D", "lat"),
          ("B", "phat")
        ), List(
          ("id", StringType, true),
          ("foo", StringType, true)
        )
      )

      val actualDF = sourceDF
        .groupBy("id")
        .agg(collect_list("foo") as "collect_list_foo")

      val expectedDF = spark.createDF(
        List(
          ("B", List("cat", "phat")),
          ("D", List("lat")),
          ("C", List("matt")),
          ("A", List("cat", "bat"))
        ), List(
          ("id", StringType, true),
          ("collect_list_foo", ArrayType(StringType, true), true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#collect_set") {

    it("Aggregate function: returns a set of objects with duplicate elements eliminated") {

      val sourceDF = spark.createDF(
        List(
          ("A", "cat"),
          ("A", "cat"),
          ("A", "cat"),
          ("A", "cat"),
          ("A", "cat"),
          ("A", "cat"),
          ("B", "cat"),
          ("A", "nice"),
          ("C", "matt"),
          ("D", "lat"),
          ("B", "hey")
        ), List(
          ("id", StringType, true),
          ("foo", StringType, true)
        )
      )

      val actualDF = sourceDF
        .groupBy("id")
        .agg(collect_set("foo") as "collect_set_foo")

      val expectedDF = spark.createDF(
        List(
          ("B", List("cat", "hey")),
          ("D", List("lat")),
          ("C", List("matt")),
          ("A", List("cat", "nice"))
        ), List(
          ("id", StringType, true),
          ("collect_set_foo", ArrayType(StringType, true), true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#column") {

    it("Returns a Column based on the given column name. Alias of col") {

      val sourceDF = spark.createDF(
        List(
          (1),
          (4),
          (99)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("column", column("num1"))

      val expectedDF = spark.createDF(
        List(
          (1, 1),
          (4, 4),
          (99, 99)
        ), List(
          ("num1", IntegerType, true),
          ("column", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#concat_ws") {

    it("concatenates multiple input string columns with separator") {

      val wordsDF = spark.createDF(
        List(
          ("banh", "mi"),
          ("pho", "ga"),
          (null, "cheese"),
          ("pizza", null),
          (null, null)
        ), List(
          ("word1", StringType, true),
          ("word2", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn(
        "yummy",
        concat_ws(
          "_",
          col("word1"),
          col("word2")
        )
      )

      val expectedDF = spark.createDF(
        List(
          ("banh", "mi", "banh_mi"),
          ("pho", "ga", "pho_ga"),
          (null, "cheese", "cheese"), // null column will be omitted
          ("pizza", null, "pizza"), // null column will be omitted
          (null, null, "") // all null columns give ""
        ), List(
          ("word1", StringType, true),
          ("word2", StringType, true),
          ("yummy", StringType, false)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#concat") {

    it("concatenates multiple input string columns together into a single string column") {

      val wordsDF = spark.createDF(
        List(
          ("banh", "mi"),
          ("pho", "ga"),
          (null, "cheese"),
          ("pizza", null),
          (null, null)
        ), List(
          ("word1", StringType, true),
          ("word2", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn(
        "yummy",
        concat(
          col("word1"),
          col("word2")
        )
      )

      val expectedDF = spark.createDF(
        List(
          ("banh", "mi", "banhmi"),
          ("pho", "ga", "phoga"),
          (null, "cheese", null),
          ("pizza", null, null),
          (null, null, null)
        ), List(
          ("word1", StringType, true),
          ("word2", StringType, true),
          ("yummy", StringType, true)
        )
      )
      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#conv") {
    pending
  }

  describe("#corr") {

    it("Aggregate function: returns the Pearson Correlation Coefficient for two columns") {

      val sourceDF = spark.createDF(
        List(
          (1, 8),
          (3, 6),
          (5, 30)
        ), List(
          ("num1", IntegerType, true),
          ("num2", IntegerType, true)
        )
      )

      val actualDF = sourceDF.agg(corr("num1", "num2"))

      val expectedDF = spark.createDF(
        List(
          (0.8260331876309022)
        ), List(
          ("corr(num1, num2)", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#cos") {

    it("calculates the cosine of the given value") {

      val sourceDF = spark.createDF(
        List(
          (1),
          (2),
          (3)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("i_am_scared", cos("num1"))

      val expectedDF = spark.createDF(
        List(
          (1, 0.5403023058681398),
          (2, -0.4161468365471424),
          (3, -0.9899924966004454)
        ), List(
          ("num1", IntegerType, true),
          ("i_am_scared", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#cosh") {

    it("Computes the hyperbolic cosine of the given column") {

      val sourceDF = spark.createDF(
        List(
          (1),
          (2),
          (3)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("cosh", cosh("num1"))

      val expectedDF = spark.createDF(
        List(
          (1, 1.543080634815244),
          (2, 3.7621956910836314),
          (3, 10.067661995777765)
        ), List(
          ("num1", IntegerType, true),
          ("cosh", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#count") {
    it("Returns the number of rows in the DataFrame.") {

      val expectedCount = 5
      val sourceDF = spark.createDF(
        List(
          ("Spotlight", 2015),
          ("Birdman", 2014),
          ("12 Years a Slave", 2013),
          ("Argo", 2012),
          ("Boyhood", 2014)
        ), List(
          ("movie", StringType, true),
          ("year", IntegerType, true)
        )
      )

      val rowCount = sourceDF.count;

      assert(rowCount, expectedCount)

    }
  }

  describe("#countDistinct") {

    it("aggregate function: returns the number of distinct items in a group") {

      val sourceDF = spark.createDF(
        List(
          ("A", 1),
          ("B", 1),
          ("A", 2),
          ("A", 2),
          ("B", 3),
          ("A", 3)
        ), List(
          ("id", StringType, true),
          ("foo", IntegerType, true)
        )
      )

      val actualDF = sourceDF.groupBy($"id").agg(countDistinct($"foo") as "distinctCountFoo").orderBy($"id")

      val expectedDF = spark.createDF(
        List(
          ("A", 3L),
          ("B", 2L)
        ), List(
          ("id", StringType, true),
          ("distinctCountFoo", LongType, false)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#covar_pop") {
    pending
  }

  describe("#covar_samp") {
    pending
  }

  describe("#crc32") {

    it("Calculates the cyclic redundancy check value (CRC32) of a binary column and returns the value as a bigint") {

      val sourceDF = spark.createDF(
        List(
          ("101001"),
          ("100011")
        ), List(
          ("bin1", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("crc32", crc32(col("bin1")))

      val expectedDF = spark.createDF(
        List(
          ("101001", 1033994672L),
          ("100011", 2617694100L)
        ), List(
          ("bin1", StringType, true),
          ("crc32", LongType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#cume_dist") {
    pending
  }

  describe("#current_date") {
    pending
  }

  describe("#current_timestamp") {
    pending
  }

  describe("#date_add") {

    it("returns the date that is days days after start") {

      val sourceDF = spark.createDF(
        List(
          ("1", Date.valueOf("2016-01-01")),
          ("2", Date.valueOf("2016-12-01"))
        ), List(
          ("person_id", StringType, true),
          ("birth_date", DateType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "future_date",
        date_add(col("birth_date"), 4)
      )

      val expectedDF = spark.createDF(
        List(
          ("1", Date.valueOf("2016-01-01"), Date.valueOf("2016-01-05")),
          ("2", Date.valueOf("2016-12-01"), Date.valueOf("2016-12-05"))
        ), List(
          ("person_id", StringType, true),
          ("birth_date", DateType, true),
          ("future_date", DateType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#date_format") {
    pending
  }

  describe("#date_sub") {
    pending
  }

  describe("#datediff") {
    pending
  }

  describe("#dayofmonth") {
    pending
  }

  describe("#dayofyear") {
    pending
  }

  describe("#decode") {
    pending
  }

  describe("#degrees") {

    it("Converts an angle measured in radians to an approximately equivalent angle measured in degrees") {

      val sourceDF = spark.createDF(
        List(
          (1.0),
          (0.1),
          (0.05),
          (0.04),
          (null)
        ), List(
          ("radian", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("degree", degrees(col("radian")))

      val expectedDF = spark.createDF(
        List(
          (1.0, 57.29577951308232),
          (0.1, 5.729577951308232),
          (0.05, 2.864788975654116),
          (0.04, 2.291831180523293),
          (null, null)
        ), List(
          ("radian", DoubleType, true),
          ("degree", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }
  }

  describe("#dense_rank") {
    pending
  }

  describe("#desc_nulls_first") {

    it("Returns a sort expression based on the descending order of the column, and null values appear before non-null values.") {

      val sourceDF = spark.createDF(
        List(
          (null),
          (1),
          (-8),
          (null),
          (-5)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.sort(desc_nulls_first("num1"))

      val expectedDF = spark.createDF(
        List(
          (null),
          (null),
          (1),
          (-5),
          (-8)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#desc_nulls_last") {

    it("Returns a sort expression based on the descending order of the column, and null values appear after non-null values.") {

      val sourceDF = spark.createDF(
        List(
          (null),
          (1),
          (7),
          (null),
          (-5)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.sort(desc_nulls_last("num1"))

      val expectedDF = spark.createDF(
        List(
          (7),
          (1),
          (-5),
          (null),
          (null)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#desc") {

    it("sorts a column in descending order") {

      val sourceDF = spark.createDF(
        List(
          (1),
          (-8),
          (-5)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.sort(desc("num1"))

      val expectedDF = spark.createDF(
        List(
          (1),
          (-5),
          (-8)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

  }

  describe("#encode") {
    pending
  }

  describe("#exp") {

    it("Computes the exponential of the given column.") {

      val sourceDF = spark.createDF(
        List(
          (0),
          (1),
          (2),
          (null)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("exp", exp(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (0, 1.0),
          (1, 2.718281828459045),
          (2, 7.38905609893065),
          (null, null)
        ), List(
          ("num1", IntegerType, true),
          ("exp", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#explode") {

    it("returns a new DataFrame where each row has been expanded to zero or more rows by the provided function") {

      val df = spark.createDF(
        List(
          ("A", List("a", "b", "c"), List(1, 2, 3)),
          ("B", List("a", "b", "c"), List(5, 6, 7))
        ), List(
          ("id", StringType, true),
          ("class", ArrayType(StringType, true), true),
          ("num", ArrayType(IntegerType, true), true)
        )
      )

      val actualDF = df.select(
        df("id"),
        explode(df("class")).alias("class"),
        df("num")
      )

      val expectedDF = spark.createDF(
        List(
          ("A", "a", Seq(1, 2, 3)),
          ("A", "b", Seq(1, 2, 3)),
          ("A", "c", Seq(1, 2, 3)),
          ("B", "a", Seq(5, 6, 7)),
          ("B", "b", Seq(5, 6, 7)),
          ("B", "c", Seq(5, 6, 7))
        ), List(
          ("id", StringType, true),
          ("class", StringType, true),
          ("num", ArrayType(IntegerType, true), true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#expm1") {

    it("Computes the exponential of the given column minus one") {

      val sourceDF = spark.createDF(
        List(
          (0),
          (1),
          (2),
          (null)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("expm1", expm1(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (0, 0.0),
          (1, 1.718281828459045),
          (2, 6.38905609893065),
          (null, null)
        ), List(
          ("num1", IntegerType, true),
          ("expm1", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#expr") {
    pending
  }

  describe("#factorial") {

    it("calculates the product of an integer and all the integers below") {

      val sourceDF = spark.createDF(
        List(
          (0),
          (1),
          (2),
          (3),
          (4),
          (5),
          (6)
        ), List(
          ("number", IntegerType, false)
        )
      )

      val actualDF = sourceDF.withColumn("result", factorial(col("number")))

      val expectedDF = spark.createDF(
        List(
          (0, 1L),
          (1, 1L),
          (2, 2L),
          (3, 6L),
          (4, 24L),
          (5, 120L),
          (6, 720L)
        ), List(
          ("number", IntegerType, false),
          ("result", LongType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#first") {
    pending
  }

  describe("#floor") {

    it("Computes the floor of the given value") {

      val sourceDF = spark.createDF(
        List(
          (-43.234),
          (0.1242),
          (1.3251),
          (22.235),
          (null)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("floor", floor(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (-43.234, -44L),
          (0.1242, 0L),
          (1.3251, 1L),
          (22.235, 22L),
          (null, null)
        ), List(
          ("num1", DoubleType, true),
          ("floor", LongType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#format_number") {
    pending
  }

  describe("#format_string") {
    pending
  }

  describe("#from_json") {
    pending
  }

  describe("#from_unixtime") {
    pending
  }

  describe("#get_json_object") {
    pending
  }

  describe("#greatest") {
    pending
  }

  describe("#grouping_id") {
    pending
  }

  describe("#grouping") {
    pending
  }

  describe("#hash") {
    pending
  }

  describe("#hex") {

    it("Computes hex value of the given column") {

      val sourceDF = spark.createDF(
        List(
          (236),
          (469),
          (183),
          (728)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn("hex", hex(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (236, "EC"),
          (469, "1D5"),
          (183, "B7"),
          (728, "2D8")
        ), List(
          ("num1", IntegerType, true),
          ("hex", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#hour") {
    val sourceDF = spark.createDF(
      List(
        (Timestamp.valueOf("2017-04-12 12:45:60")),
        (Timestamp.valueOf("2017-04-12 11:45:60")),
        (Timestamp.valueOf("2017-04-12 00:45:60")),
        (Timestamp.valueOf("2017-04-12 17:45:60")),
        (null)
      ), List(
        ("DateTime", TimestampType, true)
      )
    )

    val actualDF = sourceDF.withColumn("hours", hour($"DateTime"))

    val expectedDF = spark.createDF(
      List(
        (Timestamp.valueOf("2017-04-12 12:45:60"), 12),
        (Timestamp.valueOf("2017-04-12 11:45:60"), 11),
        (Timestamp.valueOf("2017-04-12 00:45:60"), 0),
        (Timestamp.valueOf("2017-04-12 17:45:60"), 17),
        (null, null)
      ), List(
        ("DateTime", TimestampType, true),
        ("hours", IntegerType, true)
      )
    )

    assertSmallDatasetEquality(actualDF, expectedDF)
  }

  describe("#hypot") {
    pending
  }

  describe("#initcap") {

    it("converts the first letter of each word to upper case, returns a new column") {

      val wordsDF = spark.createDF(
        List(
          ("bat man"),
          ("cat woman"),
          ("spider man")
        ), List(
          ("no_upper_words", StringType, true)
        )
      )
      val actualDF = wordsDF.withColumn("first_upper", initcap(col("no_upper_words")))

      val expectedDF = spark.createDF(
        List(
          ("bat man", "Bat Man"),
          ("cat woman", "Cat Woman"),
          ("spider man", "Spider Man")
        ), List(
          ("no_upper_words", StringType, true),
          ("first_upper", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#input_file_name") {
    pending
  }

  describe("#instr") {
    pending
  }

  describe("#isnan") {

    it("Return true if the column is NaN") {

      val sourceDF = spark.createDF(
        List(
          (Double.NaN),
          (24.0),
          (Double.NaN),
          (Double.NaN),
          (99.0)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("nanCheck", isnan(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (Double.NaN, true),
          (24.0, false),
          (Double.NaN, true),
          (Double.NaN, true),
          (99.0, false)
        ), List(
          ("num1", DoubleType, true),
          ("nanCheck", BooleanType, false)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#isnull") {

    it("checks column values for null") {

      val wordsDF = spark.createDF(
        List(
          (null),
          ("hello"),
          (null),
          (null),
          ("football")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn("nullCheck", isnull(col("word")))

      val expectedDF = spark.createDF(
        List(
          (null, true),
          ("hello", false),
          (null, true),
          (null, true),
          ("football", false)
        ), List(
          ("word", StringType, true),
          ("nullCheck", BooleanType, false)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#json_tuple") {
    pending
  }

  describe("#kurtosis") {
    pending
  }

  describe("#lag") {
    pending
  }

  describe("#last_day") {
    pending
  }

  describe("#last") {
    pending
  }

  describe("#lead") {
    pending
  }

  describe("#least") {
    pending
  }

  describe("#length") {

    it("returns the length of the column") {

      val expectedDF = spark.createDF(
        List(
          ("banh", 4),
          ("delilah", 7),
          (null, null)
        ), List(
          ("word", StringType, true),
          ("length", IntegerType, true)
        )
      )

      val wordsDF = spark.createDF(
        List(
          ("banh"),
          ("delilah"),
          (null)
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn("length", length(col("word")))

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#levenshtein") {
    pending
  }

  describe("#lit") {

    it("Creates a Column of literal value.") {

      val sourceDF = spark.createDF(
        List(
          ("Spider-man"),
          ("Batman")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("lit", lit("sucks"))

      val expectedDF = spark.createDF(
        List(
          ("Spider-man", "sucks"),
          ("Batman", "sucks")
        ), List(
          ("word", StringType, true),
          ("lit", StringType, false)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#locate") {

    it("returns index of first occurrence of search string") {

      val sourceDF = spark.createDF(
        List(
          ("Spider-man"),
          ("Batman")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("short_word", locate("man", col("word")))

      val expectedDF = spark.createDF(
        List(
          ("Spider-man", 8),
          ("Batman", 4)
        ), List(
          ("word", StringType, true),
          ("short_word", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#log") {

    it("Computes the natural logarithm of the given column") {

      val sourceDF = spark.createDF(
        List(
          (43.234),
          (1.0),
          (2.5),
          (10.0),
          (null)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("log", log(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (43.234, 3.766627222735861),
          (1.0, 0.0),
          (2.5, 0.9162907318741551),
          (10.0, 2.302585092994046),
          (null, null)
        ), List(
          ("num1", DoubleType, true),
          ("log", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#log10") {

    it("Computes the logarithm of the given value in base 10") {

      val sourceDF = spark.createDF(
        List(
          (43.234),
          (1.0),
          (2.5),
          (10.0),
          (null)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("log10", log10(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (43.234, 1.6358254182207552),
          (1.0, 0.0),
          (2.5, 0.3979400086720376),
          (10.0, 1.0),
          (null, null)
        ), List(
          ("num1", DoubleType, true),
          ("log10", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#log1p") {

    it("Computes the natural logarithm of the given column plus one.") {

      val sourceDF = spark.createDF(
        List(
          (43.234),
          (1.0),
          (2.5),
          (10.0),
          (null)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("log1p", log1p(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (43.234, 3.7894937241465296),
          (1.0, 0.6931471805599453),
          (2.5, 1.252762968495368),
          (10.0, 2.3978952727983707),
          (null, null)
        ), List(
          ("num1", DoubleType, true),
          ("log1p", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#log2") {

    it("Computes the logarithm of the given value in base 2") {

      val sourceDF = spark.createDF(
        List(
          (43.234),
          (1.0),
          (2.5),
          (10.0),
          (null)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("log2", log2(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (43.234, 5.434094415118396),
          (1.0, 0.0),
          (2.5, 1.3219280948873624),
          (10.0, 3.3219280948873626),
          (null, null)
        ), List(
          ("num1", DoubleType, true),
          ("log2", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#lower") {

    it("converts a string to lower case") {

      val wordsDF = spark.createDF(
        List(
          ("Batman"),
          ("CATWOMAN"),
          ("pikachu")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn("lower_word", lower(col("word")))

      val expectedDF = spark.createDF(
        List(
          ("Batman", "batman"),
          ("CATWOMAN", "catwoman"),
          ("pikachu", "pikachu")
        ), List(
          ("word", StringType, true),
          ("lower_word", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#lpad") {
    pending
  }

  describe("#ltrim") {

    it("Trim the spaces from left end for the specified string value") {

      val sourceDF = spark.createDF(
        List(
          ("nice   "),
          ("     cat"),
          (null),
          ("    person   ")
        ), List(
          ("word1", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "ltrim_column", ltrim(col("word1"))
      )

      val expectedDF = spark.createDF(
        List(
          ("nice   ", "nice   "),
          ("     cat", "cat"),
          (null, null),
          ("    person   ", "person   ")
        ), List(
          ("word1", StringType, true),
          ("ltrim_column", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#map") {
    pending
  }

  describe("#max") {

    it("Aggregate function: returns the maximum value of the expression in a group") {

      val sourceDF = spark.createDF(
        List(
          (56),
          (72),
          (8)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.agg(max("num1"))

      val expectedDF = spark.createDF(
        List(
          (72)
        ), List(
          ("max(num1)", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#md5") {

    it("Calculates the MD5 digest of a binary column and returns the value as a 32 character hex string") {

      val sourceDF = spark.createDF(
        List(
          ("101001"),
          ("100011")
        ), List(
          ("bin1", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("md5", md5(col("bin1")))

      val expectedDF = spark.createDF(
        List(
          ("101001", "1c76fee9418816ee07f81c9d3386f754"),
          ("100011", "09a146c8d1cfdbdb54ceb60ede93cdab")
        ), List(
          ("bin1", StringType, true),
          ("md5", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#mean") {

    it("Aggregate function: returns the average of the values in a group. Alias for avg") {

      val sourceDF = spark.createDF(
        List(
          (56),
          (85),
          (73),
          (99)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.agg(mean("num1"))

      val expectedDF = spark.createDF(
        List(
          (78.25)
        ), List(
          ("avg(num1)", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#min") {

    it("Aggregate function: returns the maximum value of the expression in a group") {

      val sourceDF = spark.createDF(
        List(
          (56),
          (72),
          (872),
          (3)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val actualDF = sourceDF.agg(min("num1"))

      val expectedDF = spark.createDF(
        List(
          (3)
        ), List(
          ("min(num1)", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#minute") {

    it("extracts the minute from a timestamp") {

      val sourceDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-01-01 00:10:00")),
          ("2", Timestamp.valueOf("1970-12-01 00:06:00"))
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true)
        )
      )

      val actualDF = sourceDF.withColumn("birth_minute", minute(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-01-01 00:10:00"), 10),
          ("2", Timestamp.valueOf("1970-12-01 00:06:00"), 6)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true),
          ("birth_minute", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#monotonically_increasing_id") {
    pending
  }

  describe("#month") {

    it("Extracts the month as an integer from a timestamp") {

      val sourceDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-09-30 00:00:00")),
          ("2", Timestamp.valueOf("2016-12-14 00:00:00")),
          ("3", null)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true)
        )
      )

      val actualDF = sourceDF.withColumn("month", month(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-09-30 00:00:00"), 9),
          ("2", Timestamp.valueOf("2016-12-14 00:00:00"), 12),
          ("3", null, null)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true),
          ("month", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("Extracts the month as an integer from a date") {

      val sourceDF = spark.createDF(
        List(
          ("1", Date.valueOf("2016-09-30")),
          ("2", Date.valueOf("2016-12-14"))
        ), List(
          ("person_id", StringType, true),
          ("birth_date", DateType, true)
        )
      )

      val actualDF = sourceDF.withColumn("month", month(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          ("1", Date.valueOf("2016-09-30"), 9),
          ("2", Date.valueOf("2016-12-14"), 12)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", DateType, true),
          ("month", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("Extracts the month as an integer from a string") {

      val sourceDF = spark.createDF(
        List(
          ("1", "2016-09-30"),
          ("2", "2016-12-14")
        ), List(
          ("person_id", StringType, true),
          ("birth_date", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("month", month(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          ("1", "2016-09-30", 9),
          ("2", "2016-12-14", 12)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", StringType, true),
          ("month", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#months_between") {
    pending
  }

  describe("#nanv1") {
    pending
  }

  describe("#negate") {
    pending
  }

  describe("#next_day") {

    it("Given a date column, returns the first date which is later than the value of the date column that is on the specified day of the week") {

      val sourceDF = spark.createDF(
        List(
          (Date.valueOf("2016-01-01")),
          (Date.valueOf("2016-12-01"))
        ), List(
          ("some_date", DateType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "next_day",
        next_day(col("some_date"), "Friday")
      )

      val expectedDF = spark.createDF(
        List(
          (Date.valueOf("2016-01-01"), Date.valueOf("2016-01-08")),
          (Date.valueOf("2016-12-01"), Date.valueOf("2016-12-02"))
        ), List(
          ("some_date", DateType, true),
          ("next_day", DateType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#not") {
    pending
  }

  describe("#ntile") {
    pending
  }

  describe("#percent_rank") {
    pending
  }

  describe("#pmod") {

    it("Returns the positive value of dividend mod divisor") {

      val sourceDF = spark.createDF(
        List(
          (27, 5),
          (80, 21),
          (60, 5)
        ), List(
          ("dividend", IntegerType, true),
          ("divisor", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "pmod",
        pmod(
          col("dividend"),
          col("divisor")
        )
      )

      val expectedDF = spark.createDF(
        List(
          (27, 5, 2),
          (80, 21, 17),
          (60, 5, 0)
        ), List(
          ("dividend", IntegerType, true),
          ("divisor", IntegerType, true),
          ("pmod", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#least") {

    it("Returns the least value of the list of values, skipping null values. This function takes at least 2 parameters. " + "It will return null iff all parameters are null.") {

      val sourceDF = spark.createDF(
        List(
          (56.1, null),
          (72.21, 4.2),
          (null, null),
          (3.1, 1.05)
        ), List(
          ("num1", DoubleType, true),
          ("num2", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("least", least(col("num1"), col("num2")))

      val expectedDF = spark.createDF(
        List(
          (56.1, null, 56.1),
          (72.21, 4.2, 4.2),
          (null, null, null),
          (3.1, 1.05, 1.05)

        ), List(
          ("num1", DoubleType, true),
          ("num2", DoubleType, true),
          ("least", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#posexplode") {
    pending
  }

  describe("#pow") {

    it("returns the value of the first argument raised to the power of the second argument") {

      val numsDF = spark.createDF(
        List(
          (2),
          (3),
          (1)
        ), List(
          ("num", IntegerType, true)
        )
      )

      val actualDF = numsDF.withColumn("power", pow(col("num"), 3))

      val expectedDF = spark.createDF(
        List(
          (2, 8.0),
          (3, 27.0),
          (1, 1.0)
        ), List(
          ("num", IntegerType, true),
          ("power", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#quarter") {

    it("Extracts the quarter as an integer from a given date/timestamp/string") {

      val sourceDF = spark.createDF(
        List(
          (Timestamp.valueOf("2016-01-01 00:00:00")),
          (Timestamp.valueOf("1970-12-01 00:00:00"))
        ), List(
          ("birth_date", TimestampType, true)
        )
      )

      val actualDF = sourceDF.withColumn("quarter", quarter(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          (Timestamp.valueOf("2016-01-01 00:00:00"), 1),
          (Timestamp.valueOf("1970-12-01 00:00:00"), 4)
        ), List(
          ("birth_date", TimestampType, true),
          ("quarter", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#radians") {

    it("Converts an angle measured in degrees to an approximately equivalent angle measured in radians") {

      val sourceDF = spark.createDF(
        List(
          ("1"),
          ("90"),
          ("180"),
          ("270")
        ), List(
          ("num1", StringType, false)
        )
      )

      val actualDF = sourceDF.withColumn("radians", radians(col("num1")))

      val expectedDF = spark.createDF(
        List(
          ("1", 0.017453292519943295),
          ("90", 1.5707963267948966),
          ("180", 3.141592653589793),
          ("270", 4.71238898038469)
        ), List(
          ("num1", StringType, false),
          ("radians", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#rand") {
    pending
  }

  describe("#randn") {
    pending
  }

  describe("#rank") {
    pending
  }

  describe("#regexp_extract") {
    pending
  }

  describe("#regexp_replace") {

    it("Replace all substrings of the specified string value that match regexp with rep.") {

      val wordsDF = spark.createDF(
        List(
          ("E401//"),
          ("C20.0//C20.1"),
          (null),
          ("124.21//")
        ), List(
          ("word1", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn("new_word1", regexp_replace(col("word1"), "//", "\\,"))

      val expectedDF = spark.createDF(
        List(
          ("E401//", "E401,"),
          ("C20.0//C20.1", "C20.0,C20.1"),
          (null, null),
          ("124.21//", "124.21,")
        ), List(
          ("word1", StringType, true),
          ("new_word1", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#repeat") {

    it("Repeats a string column n times, and returns it as a new string column") {

      val sourceDF = spark.createDF(
        List(
          ("nice"),
          ("job"),
          (null),
          ("c")
        ), List(
          ("word1", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("repeat", repeat(col("word1"), 3))

      val expectedDF = spark.createDF(
        List(
          ("nice", "nicenicenice"),
          ("job", "jobjobjob"),
          (null, null),
          ("c", "ccc")
        ), List(
          ("word1", StringType, true),
          ("repeat", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#reverse") {

    it("Reverses the string column and returns it as a new string column") {

      val sourceDF = spark.createDF(
        List(
          ("nice"),
          ("job"),
          (null),
          ("hannah")
        ), List(
          ("word1", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("reverse", reverse(col("word1")))

      val expectedDF = spark.createDF(
        List(
          ("nice", "ecin"),
          ("job", "boj"),
          (null, null),
          ("hannah", "hannah")
        ), List(
          ("word1", StringType, true),
          ("reverse", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#rint") {

    it("Returns the double value that is closest in value to the argument and is equal to a mathematical integer") {

      val sourceDF = spark.createDF(
        List(
          (1.2),
          (90.8),
          (180.4),
          (270.7)
        ), List(
          ("num1", DoubleType, false)
        )
      )

      val actualDF = sourceDF.withColumn("rint", rint(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (1.2, 1.0),
          (90.8, 91.0),
          (180.4, 180.0),
          (270.7, 271.0)
        ), List(
          ("num1", DoubleType, false),
          ("rint", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#round") {

    it("Returns the value of the column e rounded to 0 decimal places") {

      val sourceDF = spark.createDF(
        List(
          (8.1),
          (64.8),
          (3.5),
          (-27.0),
          (null)
        ), List(
          ("num1", DoubleType, true)
        )
      )

      val actualDF = sourceDF.withColumn("rounded_num", round(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (8.1, 8.0),
          (64.8, 65.0),
          (3.5, 4.0),
          (-27.0, -27.0),
          (null, null)
        ), List(
          ("num1", DoubleType, true),
          ("rounded_num", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#row_number") {
    pending
  }

  describe("#rpad") {

    it("Right-padded with pad to a length of len") {

      val wordsDF = spark.createDF(
        List(
          ("banh"),
          ("delilah"),
          (null),
          ("c")
        ), List(
          ("word1", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn("rpad_column", rpad(col("word1"), 5, "x"))

      val expectedDF = spark.createDF(
        List(
          ("banh", "banhx"),
          ("delilah", "delil"),
          (null, null),
          ("c", "cxxxx")
        ), List(
          ("word1", StringType, true),
          ("rpad_column", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#rtrim") {

    it("trims the spaces from right end for the specified string value") {

      val wordsDF = spark.createDF(
        List(
          ("nice   "),
          ("cat"),
          (null),
          ("  person         ")
        ), List(
          ("word1", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn(
        "rtrim_column", rtrim(col("word1"))
      )

      val expectedDF = spark.createDF(
        List(
          ("nice   ", "nice"),
          ("cat", "cat"),
          (null, null),
          ("  person         ", "  person")
        ), List(
          ("word1", StringType, true),
          ("rtrim_column", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#second") {

    it("Extracts the seconds as an integer from a given date/timestamp/string") {

      val sourceDF = spark.createDF(
        List(
          (Timestamp.valueOf("2016-01-01 12:00:17")),
          (Timestamp.valueOf("2016-06-07 00:10:56")),
          (Timestamp.valueOf("2016-12-05 05:05:22"))
        ), List(
          ("date", TimestampType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "second",
        second(col("date"))
      )

      val expectedDF = spark.createDF(
        List(
          (Timestamp.valueOf("2016-01-01 12:00:17"), 17),
          (Timestamp.valueOf("2016-06-07 00:10:56"), 56),
          (Timestamp.valueOf("2016-12-05 05:05:22"), 22)
        ), List(
          ("date", TimestampType, true),
          ("second", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#sha1") {

    it("Calculates the SHA-1 digest of a binary column and returns the value as a 40 character hex string") {

      val sourceDF = spark.createDF(
        List(
          ("1100"),
          ("1011"),
          (null)
        ), List(
          ("num1", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("sha1", sha1(col("num1")))

      val expectedDF = spark.createDF(
        List(
          ("1100", "b124524c4b1ade45d1deecbcdef614fadb3ec205"),
          ("1011", "dd2dfa50dc8feca1e5303a87b2c6a42db3ebe102"),
          (null, null)
        ), List(
          ("num1", StringType, true),
          ("sha1", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#sha2") {
    pending
  }

  describe("#shiftLeft") {
    pending
  }

  describe("#shiftRight") {
    pending
  }

  describe("#shiftRightUnsigned") {
    pending
  }

  describe("#signum") {

    it("Computes the signum of the given column") {

      val wordsDF = spark.createDF(
        List(
          (27),
          (-3),
          (null),
          (45),
          (0)
        ), List(
          ("word1", IntegerType, true)
        )
      )

      val actualDF = wordsDF.withColumn(
        "signum", signum(col("word1"))
      )

      val expectedDF = spark.createDF(
        List(
          (27, 1.0),
          (-3, -1.0),
          (null, null),
          (45, 1.0),
          (0, 0.0)
        ), List(
          ("word1", IntegerType, true),
          ("signum", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#sin") {
    pending
  }

  describe("#sinh") {
    pending
  }

  describe("#size") {
    pending
  }

  describe("#skewness") {
    pending
  }

  describe("#sort_array") {
    pending
  }

  describe("#soundex") {
    pending
  }

  describe("#spark_partition_id") {
    pending
  }

  describe("#split") {
    it("Splits str around pattern (pattern is a regular expression).") {

      val patternDF = spark.createDF(
        List(
          ("0^1^0.00"),
          ("0^.21"),
          (null),
          ("0^.9"),
          ("0^0.22")
        ), List(
          ("pattern", StringType, true)
        )
      )

      val actualDF = patternDF.withColumn(
        "split_column", split(col("pattern"), "\\^")
      )

      val expectedDF = spark.createDF(
        List(
          ("0^1^0.00", List("0", "1", "0.00")),
          ("0^.21", List("0", ".21")),
          (null, null),
          ("0^.9", List("0", ".9")),
          ("0^0.22", List("0", "0.22"))
        ), List(
          ("pattern", StringType, true),
          ("split_column", ArrayType(StringType, true), true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }
  }

  describe("#sqrt") {

    it("Computes the square root of the specified float value") {

      val numsDF = spark.createDF(
        List(
          (49),
          (144),
          (89)
        ), List(
          ("num1", IntegerType, true)
        )
      )

      val sqrtDF = numsDF.withColumn("sqrt_num", sqrt(col("num1")))

      val expectedDF = spark.createDF(
        List(
          (49, 7.0),
          (144, 12.0),
          (89, 9.433981132056603)
        ), List(
          ("num1", IntegerType, true),
          ("sqrt_num", DoubleType, true)
        )
      )

      assertSmallDatasetEquality(sqrtDF, expectedDF)

    }

  }

  describe("#stddev_pop") {
    pending
  }

  describe("#stddev_samp") {
    pending
  }

  describe("#stddev") {
    pending
  }

  describe("#struct") {
    pending
  }

  describe("#substring_index") {
    pending
  }

  describe("#substring") {

    it("Slices a string, starts at position pos of length len.") {

      val wordsDF = spark.createDF(
        List(
          ("Batman"),
          ("CATWOMAN"),
          ("pikachu")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn("substring_word", substring(col("word"), 0, 3))

      val expectedDF = spark.createDF(
        List(
          ("Batman", "Bat"),
          ("CATWOMAN", "CAT"),
          ("pikachu", "pik")
        ), List(
          ("word", StringType, true),
          ("substring_word", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#sum") {
    describe("(when column has only null values)") {
      it("returns null when column has only null values") {
        val sourceDF = Seq(SumNumericInput(None), SumNumericInput(None)).toDF
        val actualDF = sourceDF.agg(sum("colA") as "sum")
        val expectedDF = Seq(SumNumericOutput(None)).toDF

        assertSmallDatasetEquality(actualDF, expectedDF)
      }
    }
    describe("(when column has at-least one non null value)") {
      it("returns the sum of a real numbers") {
        val sourceDF = Seq(SumNumericInput(Some(200.50)), SumNumericInput(Some(-10.0)), SumNumericInput(Some(10)), SumNumericInput(Some(-30)), SumNumericInput(None)).toDF
        val actualDF = sourceDF.agg(sum("colA") as "sum")
        val expectedDF = Seq(SumNumericOutput(Some(170.5))).toDF

        assertSmallDatasetEquality(actualDF, expectedDF)
      }
    }
  }

  describe("#sumDistinct") {
    pending
  }

  describe("#tan") {
    pending
  }

  describe("#tanh") {
    pending
  }

  describe("#to_date") {
    pending
  }

  describe("#to_json") {
    pending
  }

  describe("#to_utc_timestamp") {
    pending
  }

  describe("#translate") {
    pending
  }

  describe("#trim") {

    it("converts a string to lower case") {

      val wordsDF = spark.createDF(
        List(
          ("bat  "),
          ("  cat")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn("short_word", trim(col("word")))

      val expectedDF = spark.createDF(
        List(
          ("bat  ", "bat"),
          ("  cat", "cat")
        ), List(
          ("word", StringType, true),
          ("short_word", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#trunc") {
    pending
  }

  describe("#udf") {
    pending
  }

  describe("#unbase64") {
    pending
  }

  describe("#unhex") {
    pending
  }

  describe("#unix_timestamp") {
    pending
  }

  describe("#upper") {

    it("converts a string to upper case") {

      val wordsDF = spark.createDF(
        List(
          ("BatmaN"),
          ("boO"),
          ("piKachu")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn("upper_word", upper(col("word")))

      val expectedDF = spark.createDF(
        List(
          ("BatmaN", "BATMAN"),
          ("boO", "BOO"),
          ("piKachu", "PIKACHU")
        ), List(
          ("word", StringType, true),
          ("upper_word", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#var_pop") {
    pending
  }

  describe("#var_samp") {
    pending
  }

  describe("#variance") {
    pending
  }

  describe("#weekofyear") {

    it("Extracts the week number as an integer from a timestamp") {

      val sourceDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-09-30 00:00:00")),
          ("2", Timestamp.valueOf("2016-12-14 00:00:00")),
          ("3", null)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true)
        )
      )

      val actualDF = sourceDF.withColumn("weekofyear", weekofyear(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-09-30 00:00:00"), 39),
          ("2", Timestamp.valueOf("2016-12-14 00:00:00"), 50),
          ("3", null, null)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true),
          ("weekofyear", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("Extracts the week number as an integer from a date") {

      val sourceDF = spark.createDF(
        List(
          ("1", Date.valueOf("2016-09-30")),
          ("2", Date.valueOf("2016-12-14"))
        ), List(
          ("person_id", StringType, true),
          ("birth_date", DateType, true)
        )
      )

      val actualDF = sourceDF.withColumn("weekofyear", weekofyear(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          ("1", Date.valueOf("2016-09-30"), 39),
          ("2", Date.valueOf("2016-12-14"), 50)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", DateType, true),
          ("weekofyear", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("Extracts the week number as an integer from a string") {

      val sourceDF = spark.createDF(
        List(
          ("1", "2016-09-30"),
          ("2", "2016-12-14")
        ), List(
          ("person_id", StringType, true),
          ("birth_date", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("weekofyear", weekofyear(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          ("1", "2016-09-30", 39),
          ("2", "2016-12-14", 50)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", StringType, true),
          ("weekofyear", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#when") {
    pending
  }

  describe("#window") {
    pending
  }

  describe("#year") {

    it("extracts the year from a timestamp") {

      val sourceDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-01-01 00:00:00")),
          ("2", Timestamp.valueOf("1970-12-01 00:00:00"))
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true)
        )
      )

      val actualDF = sourceDF.withColumn("birth_year", year(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          ("1", Timestamp.valueOf("2016-01-01 00:00:00"), 2016),
          ("2", Timestamp.valueOf("1970-12-01 00:00:00"), 1970)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", TimestampType, true),
          ("birth_year", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("extracts the year from a date") {

      val sourceDF = spark.createDF(
        List(
          ("1", Date.valueOf("2016-01-01")),
          ("2", Date.valueOf("1970-12-01"))
        ), List(
          ("person_id", StringType, true),
          ("birth_date", DateType, true)
        )
      )

      val actualDF = sourceDF.withColumn("birth_year", year(col("birth_date")))

      val expectedDF = spark.createDF(
        List(
          ("1", Date.valueOf("2016-01-01"), 2016),
          ("2", Date.valueOf("1970-12-01"), 1970)
        ), List(
          ("person_id", StringType, true),
          ("birth_date", DateType, true),
          ("birth_year", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

}

