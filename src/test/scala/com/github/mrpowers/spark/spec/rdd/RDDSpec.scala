package com.github.mrpowers.spark.spec.rdd

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.scalatest.FunSpec
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.stat.Statistics

class RDDSpec extends FunSpec with DataFrameSuiteBase with RDDComparisons {

  describe("#collect") {

    it("return an array that contains all of the elements in this RDD") {

      val xs = (1 to 3).toList
      val rdd = sc.parallelize(xs)
      assert(rdd.collect().deep === Array(1, 2, 3).deep)

    }

  }

  describe("#filter") {

    it("returns a new RDD containing only the elements that satisfy a predicate") {

      val xs = (1 to 5).toList
      val sourceRDD = sc.parallelize(xs)
      val actualRDD = sourceRDD.filter { n => n >= 3 }

      val expectedRDD = sc.parallelize(
        (3 to 5).toList
      )

      assertRDDEquals(actualRDD, expectedRDD)

    }

  }

  describe("flatMap") {

    it("return a new RDD by first applying a function to all elements of this RDD, and then flattening the results") {

      val xs = List("this is something long")
      val sourceRDD = sc.parallelize(xs)
      val actualRDD = sourceRDD.flatMap { l => l.split(" ") }

      val expectedRDD = sc.parallelize(
        List("this", "is", "something", "long")
      )

      assertRDDEquals(actualRDD, expectedRDD)

    }

  }

  describe("#intersect") {

    it("returns the intersection of this RDD and another one") {

      val xs1 = (1 to 5).toList
      val rdd1 = sc.parallelize(xs1)

      val xs2 = (4 to 8).toList
      val rdd2 = sc.parallelize(xs2)

      assert(xs1.intersect(xs2) === List(4, 5))

    }

  }

  describe("#map") {

    it("returns a new RDD by applying a function to all elements of this RDD") {

      val sourceData = List(
        ("cat"),
        ("dog"),
        ("frog")
      )
      val sourceRDD: RDD[String] = sc.parallelize(sourceData)

      val actualRDD: RDD[Int] = sourceRDD.map { l => l.length }

      val expectedData = List(
        (3),
        (3),
        (4)
      )
      val expectedRDD: RDD[Int] = sc.parallelize(expectedData)

      assertRDDEquals(actualRDD, expectedRDD)
    }

  }

  describe("#max") {

    it("returns the max of this RDD as defined by the implicit Ordering[T]") {

      val xs = (1 to 100).toList
      val rdd = sc.parallelize(xs)
      assert(rdd.max() === 100)

    }

  }

  describe("#corr") {

    it("returns the correlation between arrays") {

      val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))
      val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))
      val correlationPearson: Double = Statistics.corr(seriesX, seriesY, "pearson")
      val correlationSpearman: Double = Statistics.corr(seriesX, seriesY, "spearman")

      assert(correlationPearson === 0.8500286768773001)
      assert(correlationSpearman === 1.0000000000000002)
    }

  }

  describe("#zip") {

    it("return key-value pairs with the first element in each RDD, second element in each RDD") {

      val xRDD = sc.parallelize(List(1, 2, 3))

      val yRDD = sc.parallelize(List("a", "b", "c"))

      val actualRDD = xRDD.zip(yRDD)

      val expectedRDD = sc.parallelize(
        List((1, "a"), (2, "b"), (3, "c"))
      )

      assertRDDEquals(actualRDD, expectedRDD)
    }

  }

  describe("#randomSplit") {

    it("Randomly splits this RDD with the provided weights") {

      val xRDD = sc.parallelize(List.range(1, 101))

      val splits = xRDD.randomSplit(Array(0.7, 0.3), seed = 4567)

      val (trainingData, testData) = (splits(0), splits(1))

      val trainingDataSize = trainingData.count()

      //It doesn't split the data exactly 7:3
      assert(trainingDataSize === 73)
    }

  }

}
