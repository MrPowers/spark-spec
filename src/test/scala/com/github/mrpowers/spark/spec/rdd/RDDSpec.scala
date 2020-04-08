package com.github.mrpowers.spark.spec.rdd

import com.github.mrpowers.spark.fast.tests.RDDComparer
import org.scalatest.FunSpec
import org.apache.spark.rdd.RDD
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper

class RDDSpec extends FunSpec with RDDComparer with SparkSessionTestWrapper {

  describe("#aggregate") {
    pending
  }

  describe("#cache") {
    pending
  }

  describe("#cartesian") {
    pending
  }

  describe("#checkpoint") {
    pending
  }

  describe("#coalesce") {
    pending
  }

  describe("#collect") {

    it("return an array that contains all of the elements in this RDD") {

      val xs = (1 to 3).toList
      val rdd = spark.sparkContext.parallelize(xs)
      assert(rdd.collect().deep === Array(1, 2, 3).deep)

    }

  }

  describe("#compute") {
    pending
  }

  describe("#context") {
    pending
  }

  describe("#count") {
    pending
  }

  describe("#countApprox") {
    pending
  }

  describe("#countApproxDistinct") {
    pending
  }

  describe("#countByValue") {
    pending
  }

  describe("#countByValueApprox") {
    pending
  }

  describe("#dependencies") {
    pending
  }

  describe("#distinct") {
    pending
  }

  describe("#doubleRDDToDoubleRDDFunctions") {
    pending
  }

  describe("#filter") {

    it("returns a new RDD containing only the elements that satisfy a predicate") {

      val xs = (1 to 5).toList
      val sourceRDD = spark.sparkContext.parallelize(xs)
      val actualRDD = sourceRDD.filter { n => n >= 3 }

      val expectedRDD = spark.sparkContext.parallelize(
        (3 to 5).toList
      )

      assertSmallRDDEquality(actualRDD, expectedRDD)

    }

  }

  describe("#first") {
    pending
  }

  describe("flatMap") {

    it("return a new RDD by first applying a function to all elements of this RDD, and then flattening the results") {

      val xs = List("this is something long")
      val sourceRDD = spark.sparkContext.parallelize(xs)
      val actualRDD = sourceRDD.flatMap { l => l.split(" ") }

      val expectedRDD = spark.sparkContext.parallelize(
        List("this", "is", "something", "long")
      )

      assertSmallRDDEquality(actualRDD, expectedRDD)

    }

  }

  describe("#fold") {
    pending
  }

  describe("#foreach") {
    pending
  }

  describe("#foreachPartition") {
    pending
  }

  describe("#getCheckpointFile") {
    pending
  }

  describe("#getNumPartitions") {
    pending
  }

  describe("#getStorageLevel") {
    pending
  }

  describe("#glom") {
    pending
  }

  describe("#groupBy") {
    pending
  }

  describe("#id") {
    pending
  }

  describe("#intersect") {

    it("returns the intersection of this RDD and another one") {

      val xs1 = (1 to 5).toList
      val rdd1 = spark.sparkContext.parallelize(xs1)

      val xs2 = (4 to 8).toList
      val rdd2 = spark.sparkContext.parallelize(xs2)

      assert(xs1.intersect(xs2) === List(4, 5))

    }

  }

  describe("#isCheckpointed") {
    pending
  }

  describe("#isEmpty") {
    pending
  }

  describe("#iterator") {
    pending
  }

  describe("#keyBy") {
    pending
  }

  describe("#localCheckpoint") {
    pending
  }

  describe("#map") {

    it("returns a new RDD by applying a function to all elements of this RDD") {

      val sourceData = List(
        ("cat"),
        ("dog"),
        ("frog")
      )
      val sourceRDD: RDD[String] = spark.sparkContext.parallelize(sourceData)

      val actualRDD: RDD[Int] = sourceRDD.map { l => l.length }

      val expectedData = List(
        (3),
        (3),
        (4)
      )
      val expectedRDD: RDD[Int] = spark.sparkContext.parallelize(expectedData)

      assertSmallRDDEquality(actualRDD, expectedRDD)

    }

  }

  describe("#max") {

    it("returns the max of this RDD as defined by the implicit Ordering[T]") {

      val xs = (1 to 100).toList
      val rdd = spark.sparkContext.parallelize(xs)
      assert(rdd.max() === 100)

    }

  }

  describe("#mapPartitions") {
    pending
  }

  describe("#mapPartitionsWithIndex") {
    pending
  }

  describe("#max") {
    pending
  }

  describe("#min") {
    pending
  }

  describe("#name") {
    pending
  }

  describe("#numericRDDToDoubleRDDFunctions") {
    pending
  }

  describe("#partitioner") {
    pending
  }

  describe("#partitions") {
    pending
  }

  describe("#persist") {
    pending
  }

  describe("#pipe") {
    pending
  }

  describe("#preferredLocations") {
    pending
  }

  describe("#randomSplit") {
    pending
  }

  describe("#rddToAsyncRDDActions") {
    pending
  }

  describe("#rddToOrderedRDDFunctions") {
    pending
  }

  describe("#rddToPairRDDFunctions") {
    pending
  }

  describe("#rddToSequenceFileRDDFunctions") {
    pending
  }

  describe("#reduce") {
    pending
  }

  describe("#repartition") {
    pending
  }

  describe("#sample") {
    pending
  }

  describe("#saveAsObjectFile") {
    pending
  }

  describe("#saveAsTextFile") {
    pending
  }

  describe("#setName") {
    pending
  }

  describe("#sortBy") {
    pending
  }

  describe("#sparkContext") {
    pending
  }

  describe("#subtract") {
    pending
  }

  describe("#take") {
    pending
  }

  describe("#takeOrdered") {
    pending
  }

  describe("#takeSample") {
    pending
  }

  describe("#toDebugString") {
    pending
  }

  describe("#toJavaRDD") {
    pending
  }

  describe("#toLocalIterator") {
    pending
  }

  describe("#top") {
    pending
  }

  describe("#toString") {
    pending
  }

  describe("#treeAggregate") {
    pending
  }

  describe("#treeReduce") {
    pending
  }

  describe("#union") {
    pending
  }

  describe("#unpersist") {
    pending
  }

  describe("#zip") {

    it("return key-value pairs with the first element in each RDD, second element in each RDD") {

      val xRDD = spark.sparkContext.parallelize(List(1, 2, 3))

      val yRDD = spark.sparkContext.parallelize(List("a", "b", "c"))

      val actualRDD = xRDD.zip(yRDD)

      val expectedRDD = spark.sparkContext.parallelize(
        List(
          (1, "a"),
          (2, "b"),
          (3, "c")
        )
      )

      assertSmallRDDEquality(actualRDD, expectedRDD)

    }

  }

  describe("#zipPartitions") {
    pending
  }

  describe("#zipWithIndex") {
    pending
  }

  describe("#zipWithUniqueId") {
    pending
  }

}
