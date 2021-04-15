package com.github.mrpowers.spark.spec.sql

import com.github.mrpowers.spark.fast.tests.{DatasetComparer, RDDComparer}
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Column, Dataset, Row}
import org.scalatest._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.storage.StorageLevel

class DatasetSpec
  extends FunSpec
  with SparkSessionTestWrapper
  with DatasetComparer
  with RDDComparer {

  import spark.implicits._

  describe("#agg") {
    pending
  }

  describe("#alias") {
    pending
  }

  describe("#apply") {
    pending
  }

  describe("#as") {

    it("does the same thing as alias") {

      val sourceDF = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("name")

      val actualDF = sourceDF.select(col("name").as("student"))

      val expectedDF = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("student")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#cache") {

    it("Should cache in memory only") {
      val sourceDF = Seq(("jose"), ("ki"), ("luisa")).toDF("name")

      val cachedDF = sourceDF.cache()

      assertSmallDatasetEquality(sourceDF, cachedDF)

      cachedDF.unpersist()

    }

  }

  describe("#checkpoint") {
    pending
  }

  describe("#classTag") {
    pending
  }

  describe("#coalesce") {
    pending
  }

  describe("#col") {
    pending
  }

  describe("#collect") {

    it("returns an array of Rows in the DataFrame") {

      val row1 = Row("cat")
      val row2 = Row("dog")

      val sourceDF = spark.createDF(
        List(
          row1,
          row2
        ), List(
          StructField("animal", StringType, true)
        )
      )

      val s = sourceDF.collect()

      assert(s === Array(row1, row2))

    }

  }

  describe("#collectAsList") {
    pending
  }

  describe("#columns") {

    it("returns all the column names as an array") {

      val sourceDF = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val expected = Array("team", "sport")

      assert(sourceDF.columns === expected)

    }

  }

  describe("#count") {
    pending
  }

  describe("#createGlobalTempView") {
    pending
  }

  describe("#createOrReplaceTempView") {
    pending
  }

  describe("#createTempView") {
    pending
  }

  describe("#crossJoin") {

    it("cross joins two DataFrames") {

      val letterDF = Seq(
        ("a"),
        ("b")
      ).toDF("letter")

      val numberDF = Seq(
        ("1"),
        ("2")
      ).toDF("number")

      val actualDF = letterDF.crossJoin(numberDF)

      val expectedDF = Seq(
        ("a", "1"),
        ("a", "2"),
        ("b", "1"),
        ("b", "2")
      ).toDF("letter", "number")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#cube") {
    pending
  }

  describe("#describe") {

    it("provides analytic statistics for a numeric column") {

      val numbersDF = Seq(
        (1),
        (8),
        (5)
      ).toDF("num1")

      val actualDF = numbersDF.describe()

      val expectedDF = Seq(
        ("count", "3"),
        ("mean", "4.666666666666667"),
        ("stddev", "3.5118845842842465"),
        ("min", "1"),
        ("max", "8")
      ).toDF("summary", "num1")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("only provides certain descriptive stats for a string column") {

      val letterDF = Seq(
        ("a"),
        ("b")
      ).toDF("letter")

      val actualDF = letterDF.describe()

      val expectedDF = Seq(
        ("count", "2"),
        ("mean", null),
        ("stddev", null),
        ("min", "a"),
        ("max", "b")
      ).toDF("summary", "letter")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#distinct") {

    it("returns the unique rows in a DataFrame") {

      val numbersDF = Seq(
        (1, 2),
        (8, 8),
        (1, 2),
        (5, 6),
        (8, 8)
      ).toDF("num1", "num2")

      val actualDF = numbersDF.distinct()

      val expectedDF = Seq(
        (1, 2),
        (5, 6),
        (8, 8)
      ).toDF("num1", "num2")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#drop") {

    it("drops a column from a DataFrame") {

      val peopleDF = Seq(
        ("larry", true),
        ("jeff", false),
        ("susy", false)
      ).toDF("person", "wearGlasses")

      val actualDF = peopleDF.drop("wearGlasses")

      val expectedDF = Seq(
        ("larry"),
        ("jeff"),
        ("susy")
      ).toDF("person")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#dropDuplicates") {

    it("drops the duplicate rows from a DataFrame") {

      val numbersDF = spark.createDF(
        List(
          (1, 2),
          (8, 8),
          (1, 2),
          (5, 6),
          (8, 8)
        ), List(
          ("num1", IntegerType, true),
          ("num2", IntegerType, true)
        )
      )

      val actualDF = numbersDF.dropDuplicates()

      val expectedDF = spark.createDF(
        List(
          (1, 2),
          (5, 6),
          (8, 8)
        ), List(
          ("num1", IntegerType, true),
          ("num2", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("drops duplicate rows based on certain columns") {

      val sourceDF = spark.createDF(
        List(
          (1, 2, 100),
          (8, 8, 100),
          (1, 2, 200),
          (5, 6, 7),
          (8, 8, 50)
        ), List(
          ("num1", IntegerType, true),
          ("num2", IntegerType, true),
          ("num3", IntegerType, true)
        )
      )

      val actualDF = sourceDF.dropDuplicates("num1", "num2")

      val expectedDF = spark.createDF(
        List(
          Row(1, 2, 100),
          Row(5, 6, 7),
          Row(8, 8, 100)
        ), List(
          StructField("num1", IntegerType, true),
          StructField("num2", IntegerType, true),
          StructField("num3", IntegerType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#dtypes") {

    it("returns the column names and their data types as an array") {

      val abcDF = Seq(
        ("a", 1),
        ("b", 2),
        ("c", 3)
      ).toDF("letter", "number")

      val actual = abcDF.dtypes
      val expected = Array(("letter", StringType), ("number", IntegerType))

      pending
      // HACK - couldn't get this to work
      // Don't know how to do Array equality with Scala

      // actual.deep should equal(expected.deep)

    }

  }

  describe("#except") {

    it("returns a new Dataset with the rows in this Dataset but not in another Dataset") {

      val numbersDF = Seq(
        (1, 2),
        (4, 5),
        (8, 9)
      ).toDF("num1", "num2")

      val moreDF = Seq(
        (100, 200),
        (4, 5),
        (800, 900)
      ).toDF("num1", "num2")

      val actualDF = numbersDF.except(moreDF)

      val expectedDF = Seq(
        (8, 9),
        (1, 2)
      ).toDF("num1", "num2")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#explain") {
    pending
  }

  describe("#filter") {

    it("filters rows based on a given condition") {

      val numbersDF = Seq(
        (1),
        (4),
        (8),
        (42)
      ).toDF("num1")

      val actualDF = numbersDF.filter(col("num1") > 5)

      val expectedDF = Seq(
        (8),
        (42)
      ).toDF("num1")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("filters rows based on a SQL condition") {

      val numbersDF = Seq(
        (1),
        (4),
        (8),
        (42)
      ).toDF("num1")

      val actualDF = numbersDF.filter("num1 != 8")

      val expectedDF = Seq(
        (1),
        (4),
        (42)
      ).toDF("num1")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("returns a new Dataset that only contains elements where func returns true") {

      val numbersDF = Seq(
        (1),
        (4),
        (8),
        (42)
      ).toDF("num1")

      val x: Row => Boolean = (r: Row) => r(0) != 8

      val actualDF = numbersDF.filter(x)

      val expectedDF = Seq(
        (1),
        (4),
        (42)
      ).toDF("num1")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("returns a new Dataset using filtering") {
      val sourceDS = Seq(
        PersonWithAge("Alice", 29),
        PersonWithAge("Bob", 17)
      ).toDS

      val actualDS = sourceDS.filter(col("age") > 18)

      val expectedDS = Seq(
        PersonWithAge("Alice", 29)
      ).toDS

      assertSmallDatasetEquality(actualDS, expectedDS)
    }

  }

  describe("#first") {

    it("returns the first row of a DataFrame") {

      val row1 = Row("doug")
      val row2 = Row("patty")

      val sourceDF = spark.createDF(
        List(
          row1,
          row2
        ), List(
          StructField("character", StringType, true)
        )
      )

      assert(sourceDF.first() === row1)

    }

  }

  describe("#flatMap") {

    it("replaces explode and provides flexibility") {
      pending
    }

  }

  describe("#foreach") {
    pending
  }

  describe("#foreachPartition") {
    pending
  }

  describe("#groupBy") {

    it("groups columns for aggregations") {

      val playersDF = Seq(
        (1, "boston"),
        (4, "boston"),
        (8, "detroit"),
        (42, "detroit")
      ).toDF("score", "team")

      val actualDF = playersDF.groupBy("team").sum("score")

      val expectedDF = spark.createDF(
        List(
          Row("boston", 5.toLong),
          Row("detroit", 50.toLong)
        ), List(
          StructField("team", StringType, true),
          StructField("sum(score)", LongType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#groupByKey") {
    pending
  }

  describe("#head") {

    it("returns the first row") {

      val row1 = Row("doug")
      val row2 = Row("patty")

      val sourceDF = spark.createDF(
        List(
          row1,
          row2
        ), List(
          StructField("character", StringType, true)
        )
      )

      assert(sourceDF.head() === row1)

    }

    it("returns the first n rows") {

      val row1 = Row("doug")
      val row2 = Row("patty")
      val row3 = Row("frank")

      val sourceDF = spark.createDF(
        List(
          row1,
          row2
        ), List(
          StructField("character", StringType, true)
        )
      )

      assert(sourceDF.head(2) === Array(row1, row2))

    }

  }

  describe("#inputFiles") {
    pending
  }

  describe("#intersect") {

    it("returns a DataFrames that contains the rows in both the DataFrames") {

      val numbersDF = Seq(
        (1, 2),
        (4, 5),
        (8, 9)
      ).toDF("num1", "num2")

      val moreDF = Seq(
        (100, 200),
        (4, 5),
        (800, 900),
        (1, 2)
      ).toDF("num1", "num2")

      val actualDF = numbersDF.intersect(moreDF)

      val expectedDF = Seq(
        (1, 2),
        (4, 5)
      ).toDF("num1", "num2")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#isLocal") {
    pending
  }

  describe("#isStreaming") {
    pending
  }

  describe("#javaRDD") {
    pending
  }

  describe("#join") {

    it("joins two DataFrames") {

      val peopleDF = Seq(
        ("larry", "1"),
        ("jeff", "2"),
        ("susy", "3")
      ).toDF("person", "id")

      val birthplaceDF = Seq(
        ("new york", "1"),
        ("ohio", "2"),
        ("los angeles", "3")
      ).toDF("city", "person_id")

      val actualDF = peopleDF.join(
        birthplaceDF, peopleDF("id") <=> birthplaceDF("person_id")
      )

      val expectedDF = Seq(
        ("larry", "1", "new york", "1"),
        ("jeff", "2", "ohio", "2"),
        ("susy", "3", "los angeles", "3")
      ).toDF("person", "id", "city", "person_id")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#joinWith") {

    it("joins two DataFrames") {

      val peopleDF = spark.createDF(
        List(
          ("larry", "1"),
          ("jeff", "2")
        ), List(
          ("person", StringType, true),
          ("id", StringType, true)
        )
      )

      val birthplaceDF = spark.createDF(
        List(
          ("new york", "1"),
          ("ohio", "2")
        ), List(
          ("city", StringType, true),
          ("person_id", StringType, true)
        )
      )

      val actualDF = peopleDF.joinWith(
        birthplaceDF, peopleDF("id") <=> birthplaceDF("person_id")
      )

      //      StructType(
      //        StructField(
      //          _1,
      //          StructType(
      //            StructField(person,StringType,true),
      //            StructField(id,StringType,true)
      //          ), false),
      //        StructField(
      //          _2,
      //          StructType(
      //            StructField(city,StringType,true),
      //            StructField(person_id,StringType,true)
      //          ), false
      //        )
      //      )

      //      val st1 = StructType(
      //        List(
      //          StructField("person", StringType, true),
      //          StructField("id", StringType, true)
      //        )
      //      )
      //
      //      val st2 = StructType(
      //        List(
      //          StructField("city", StringType, true),
      //          StructField("person_id", StringType, true)
      //        )
      //      )
      //
      //      val expectedSchema = StructType(
      //        List(
      //          StructField("_1", st1, false),
      //          StructField("_2", st2, false)
      //        )
      //      )
      //
      //      val st1Data = Seq(
      //        Row("larry", "1"),
      //        Row("jeff", "2")
      //      )
      //
      //      val st2Data = Seq(
      //        Row("new york", "1"),
      //        Row("ohio", "2")
      //      )
      //
      //      val l1 = spark.createDF(
      //        List(
      //          ("larry", "1")
      //        ), List(
      //          ("person", StringType, true),
      //          ("id", StringType, true)
      //        )
      //      )
      //
      //      val ny1 = spark.createDF(
      //        List(
      //          ("new york", "1")
      //        ), List(
      //          ("city", StringType, true),
      //          ("person_id", StringType, true)
      //        )
      //      )
      //
      //      val expectedData = Seq(
      //        Row(Seq(Row("larry", "1"), Row("new york", "1"))),
      //        Row(Seq(Row("jeff", "2"), Row("ohio", "2")))
      //      )
      //
      //      val expectedDF = spark.createDataFrame(
      //        spark.sparkContext.parallelize(expectedData),
      //        StructType(expectedSchema)
      //      )

      pending

      // HACK - FAIL
      // This Stackoverflow question might help: http://stackoverflow.com/questions/36731674/re-using-a-schema-from-json-within-a-spark-dataframe-using-scala

    }

  }

  describe("#limit") {

    it("takes the first n rows of a Dataset") {

      val citiesDF = Seq(
        (true, "boston"),
        (true, "bangalore"),
        (true, "bogota"),
        (false, "dubai")
      ).toDF("have_visited", "city")

      val actualDF = citiesDF.limit(2)

      val expectedDF = Seq(
        (true, "boston"),
        (true, "bangalore")
      ).toDF("have_visited", "city")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#map") {
    pending
  }

  describe("#mapPartitions") {
    pending
  }

  describe("#na") {

    it("provides functionality for working with missing data") {

      val sourceDF = spark.createDF(
        List(
          Row(null, "boston"),
          Row(null, null),
          Row(true, "bogota"),
          Row(false, "dubai")
        ), List(
          StructField("have_visited", BooleanType, true),
          StructField("city", StringType, true)
        )
      )

      val actualDF = sourceDF.na.drop()

      val expectedDF = spark.createDF(
        List(
          Row(true, "bogota"),
          Row(false, "dubai")
        ), List(
          StructField("have_visited", BooleanType, true),
          StructField("city", StringType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#ofRows") {
    pending
  }

  describe("#orderBy") {

    it("orders the numbers in a DataFrame") {

      val numbersDF = Seq(
        99,
        4,
        55,
        42
      ).toDF("num1")

      val actualDF = numbersDF.orderBy("num1")

      val expectedDF = Seq(
        4,
        42,
        55,
        99
      ).toDF("num1")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#persist") {
    pending
  }

  describe("#printSchema") {
    pending
  }

  describe("#queryExecution") {
    pending
  }

  describe("#randomSplit") {

    it("splits a DataFrame into n different DataFrames with specified weights") {

      val numbersDF = Seq(
        99,
        4,
        55,
        42
      ).toDF("num1")

      val actual = numbersDF.randomSplit(Array(0.5, 0.5))

      assert(actual.size === 2)

    }

  }

  describe("#randomSplitAsList") {
    pending
  }

  describe("#rdd") {

    it("converts a DataFrame to a RDD") {

      val stuffDF = Seq(
        "bag",
        "shirt"
      ).toDF("thing")

      val stuffRDD = stuffDF.rdd

      val l: List[org.apache.spark.sql.Row] = List(
        Row("bag"),
        Row("shirt")
      )

      val expectedRDD = spark.sparkContext.parallelize(l)

      assertSmallRDDEquality(stuffRDD, expectedRDD)

    }

  }

  describe("#reduce") {
    pending
  }

  describe("#repartition") {

    it("changes the number of partitions in a DataFrame") {

      val stuffDF = Seq(
        "bag",
        "shirt"
      ).toDF("thing")

      val processedDF = stuffDF.repartition(8)

      assert(processedDF.rdd.partitions.length === 8)

    }

  }

  describe("#rollup") {

    it("creates a rollup with one variable") {

      val sourceData = Seq(
        ("1", "A", 1000),
        ("2", "A", 2000),
        ("1", "B", 2000),
        ("2", "B", 4000)
      ).toDF("department", "group", "money")

      val actualDF = sourceData.rollup(col("group")).sum().withColumnRenamed("sum(money)", "money").orderBy(col("group"))

      val expectedDF = spark.createDF(
        List(
          Row(null, 9000L),
          Row("A", 3000L),
          Row("B", 6000L)
        ), List(
          StructField("group", StringType, true),
          StructField("money", LongType, true)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("creates a multi-dimensional rollup") {

      // stole example from this question: http://stackoverflow.com/questions/37975227/what-is-the-difference-between-cube-and-groupby-for-operating-on-dataframes

      val df = Seq(
        ("foo", 1),
        ("foo", 2),
        ("bar", 2),
        ("bar", 2)
      ).toDF("x", "y")

      val actualDF = df.rollup($"x", $"y").count()

      val expectedDF = spark.createDF(
        List(
          Row("bar", 2, 2L),
          Row(null, null, 4L),
          Row("foo", 1, 1L),
          Row("foo", 2, 1L),
          Row("foo", null, 2L),
          Row("bar", null, 2L)
        ), List(
          StructField("x", StringType, true),
          StructField("y", IntegerType, true),
          StructField("count", LongType, false)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#sample") {

    it("returns a sample of the new Dataset") {

      val df = Seq(
        ("foo", 1),
        ("foo", 2),
        ("bar", 2),
        ("bar", 2)
      ).toDF("x", "y")

      val actualDF = df.sample(true, 0.25)

      assert(actualDF.count < 4)

    }

  }

  describe("#schema") {

    it("returns the schema of a Dataset") {

      val df = Seq(
        ("foo", 1),
        ("foo", 2)
      ).toDF("x", "y")

      val expectedSchema = StructType(
        List(
          StructField("x", StringType, true),
          StructField("y", IntegerType, false)
        )
      )

      assert(df.schema === expectedSchema)

    }

  }

  describe("#select") {
    pending
  }

  describe("#selectExpr") {
    pending
  }

  describe("#show") {
    pending
  }

  describe("#sort") {

    it("sorts a DataFrame with one column") {

      val sourceDF = Seq(
        (5),
        (1)
      ).toDF("number")

      val actualDF = sourceDF.sort("number")

      val expectedDF = Seq(
        (1),
        (5)
      ).toDF("number")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("sorts a DataFrame with multiple columns") {

      val sourceDF = Seq(
        (5, "bob"),
        (1, "phil"),
        (5, "anne")
      ).toDF("number", "name")

      val cols: List[Column] = List(col("number"), col("name"))
      val actualDF = sourceDF.sort(cols: _*)

      val expectedDF = Seq(
        (1, "phil"),
        (5, "anne"),
        (5, "bob")
      ).toDF("number", "name")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }
  }

  describe("#sortWithinPartitions") {
    pending
  }

  describe("#sparkSession") {
    pending
  }

  describe("#sqlContext") {
    pending
  }

  describe("#stat") {
    pending
  }

  describe("#storageLevel") {

    it("Returns the  storage level used with persist()") {

      val sourceDF = Seq(
        (5, "bob"),
        (1, "phil"),
        (5, "anne")
      ).toDF("number", "name")

      assert(sourceDF.persist(StorageLevel.DISK_ONLY).storageLevel === StorageLevel.DISK_ONLY)
      //clean up
      sourceDF.unpersist()
    }

    it("Returns the defaiult storage level StorageLevel.NONE when not persisted") {

      val sourceDF = Seq(
        (5, "bob"),
        (1, "phil"),
        (5, "anne")
      ).toDF("number", "name")

      assert(sourceDF.storageLevel === StorageLevel.NONE)
    }

    it("Returns the  storage level StorageLevel.MEMORY_AND_DISK when cache or persist") {

      val sourceDF = Seq(
        (5, "bob"),
        (1, "phil"),
        (5, "anne")
      ).toDF("number", "name")

      assert(sourceDF.cache().storageLevel === StorageLevel.MEMORY_AND_DISK)
      //cleanup
      sourceDF.unpersist()
    }

  }

  describe("#take") {

    it("Returns an array of the first n GenericRowWithSchema  to the driver") {
      val sourceDF = Seq(
        (5, "bob"),
        (1, "phil"),
        (5, "anne")
      ).toDF("number", "name")

      val rowsTaken = sourceDF.take(1)

      val expectedRowsTaken = Array(new GenericRowWithSchema(Array(5, "bob"), sourceDF.schema))

      assert(rowsTaken === expectedRowsTaken)
    }
  }

  describe("#takeAsList") {
    it("Returns an List of the first n GenericRowWithSchema  to the driver") {
      val sourceDF = Seq(
        (5, "bob"),
        (1, "phil"),
        (5, "anne")
      ).toDF("number", "name")

      val rowsTaken = sourceDF.takeAsList(1)

      val expectedRowsTaken = new java.util.ArrayList[GenericRowWithSchema]()
      expectedRowsTaken.add(new GenericRowWithSchema(Array(5, "bob"), sourceDF.schema))

      assert(rowsTaken === expectedRowsTaken)
    }
  }

  describe("#toDF") {
    pending
  }

  describe("#toJavaRDD") {
    pending
  }

  describe("#toJSON") {
    pending
  }

  describe("#toLocalIterator") {
    pending
  }

  describe("#toString") {
    pending
  }

  describe("#transform") {
    pending
  }

  describe("#union") {
    it("combines entries of datasets with same schema") {
      val juniorParticipants = Seq(
        GameComment("Alice Jr", 12, Some("Good game")),
        GameComment("Bob Jr", 17, None)
      ).toDS

      val seniorParticipants = Seq(
        GameComment("Alice Sr", 52, Some("Good Play")),
        GameComment("Bob Sr", 47, None)
      ).toDS

      val actualDS = juniorParticipants.union(seniorParticipants)

      val expectedDS = Seq(
        GameComment("Alice Jr", 12, Some("Good game")),
        GameComment("Bob Jr", 17, None),
        GameComment("Alice Sr", 52, Some("Good Play")),
        GameComment("Bob Sr", 47, None)
      ).toDS

      assertSmallDatasetEquality(actualDS, expectedDS)
    }

    it("combines entries of datasets with same schema and keeps duplicates unlike union in sql") {
      val juniorParticipants = Seq(
        GameComment("Alice Jr", 12, Some("Good game")),
        GameComment("Mindy", 22, None),
        GameComment("Bob Jr", 17, None)
      ).toDS

      val seniorParticipants = Seq(
        GameComment("Alice Sr", 52, Some("Good Play")),
        GameComment("Bob Sr", 47, None),
        GameComment("Mindy", 22, None)
      ).toDS

      val actualDS = juniorParticipants.union(seniorParticipants)

      val expectedDS = Seq(
        GameComment("Alice Jr", 12, Some("Good game")),
        GameComment("Mindy", 22, None),
        GameComment("Bob Jr", 17, None),
        GameComment("Alice Sr", 52, Some("Good Play")),
        GameComment("Bob Sr", 47, None),
        GameComment("Mindy", 22, None)
      ).toDS
      assertSmallDatasetEquality(actualDS, expectedDS)
    }
  }

  describe("#unpersist") {
    it("persist then unpersist") {

      val sourceDS: Dataset[PersonWithAge] = Seq(
        PersonWithAge("Alice", 12),
        PersonWithAge("Bob", 42),
        PersonWithAge("Cody", 10),
        PersonWithAge("Dane", 50)
      ).toDS

      val persisted = sourceDS.persist()
      //removes blocks from memory and disk
      val unpersisted = sourceDS.unpersist()

      assertSmallDatasetEquality(persisted, unpersisted)

    }
  }

  describe("#where") {

    it("filters rows using sql expression") {

      val sourceDS: Dataset[PersonWithAge] = Seq(
        PersonWithAge("Alice", 12),
        PersonWithAge("Bob", 42),
        PersonWithAge("Cody", 10),
        PersonWithAge("Dane", 50)
      ).toDS

      val actualDS: Dataset[PersonWithAge] = sourceDS.where("age BETWEEN 18 AND 45")

      val expectedDS: Dataset[PersonWithAge] = Seq(
        PersonWithAge("Bob", 42)
      ).toDS

      assertSmallDatasetEquality(actualDS, expectedDS)

    }

  }

  describe("#withColumn") {
    it("adds a column") {

      val sourceDF = Seq(1).toDF("foo")

      val actualDF = sourceDF.withColumn("bar", lit(2))

      val expectedDF = Seq((1, 2)).toDF("foo", "bar")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("replaces an existing column") {

      val sourceDF = Seq(1).toDF("foo")

      val actualDF = sourceDF.withColumn("foo", lit(2))

      val expectedDF = Seq(2).toDF("foo")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }
  }

  describe("#withColumnRenamed") {

    it("renames a column") {

      val sourceDF = Seq(1).toDF("foo")

      val actualDF = sourceDF.withColumnRenamed("foo", "bar")

      val expectedDF = Seq(1).toDF("bar")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

    it("does nothing if the column does not exists") {

      val sourceDF = Seq(1).toDF("foo")

      val actualDF = sourceDF.withColumnRenamed("somethingElse", "bar")

      val expectedDF = Seq(1).toDF("foo")

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#withWatermark") {
    pending
  }

  describe("#write") {
    pending
  }

  describe("#writeStream") {
    pending
  }

}
