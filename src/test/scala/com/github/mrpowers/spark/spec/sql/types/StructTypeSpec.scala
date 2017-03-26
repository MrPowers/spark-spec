package com.github.mrpowers.spark.spec.sql.types

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec

class StructTypeSpec extends FunSpec {

  describe("#new") {

    it("can be instantiated with a list of StructField arguments") {

      val s =
        StructType(
          StructField("a", IntegerType, true) ::
            StructField("b", StringType, false) ::
            StructField("c", StringType, false) :: Nil
        )
      assert(s.getClass().getName() === "org.apache.spark.sql.types.StructType")

    }

    it("can be instantiated with an explicit list of StructField arguments") {

      val s = StructType(
        List(
          StructField("a", IntegerType, true),
          StructField("b", StringType, false),
          StructField("c", StringType, false)
        )
      )
      assert(s.getClass().getName() === "org.apache.spark.sql.types.StructType")

    }

  }

  describe("add") {

    it("adds a StructField to a StructType") {

      val s1 = StructType(
        List(
          StructField("a", IntegerType, true)
        )
      )

      val actual = s1.add("dogs", StringType, true)

      val expected = StructType(
        List(
          StructField("a", IntegerType, true),
          StructField("dogs", StringType, true)
        )
      )

      assert(actual === expected)

    }

  }

  describe("addString") {
    pending
  }

  describe("aggregate") {
    pending
  }

  describe("andThen") {
    pending
  }

  describe("apply") {
    pending
  }

  describe("applyOrElse") {
    pending
  }

  describe("canEqual") {
    pending
  }

  describe("catalogString") {
    pending
  }

  describe("collect") {
    pending
  }

  describe("collectFirst") {
    pending
  }

  describe("combinations") {
    pending
  }

  describe("companion") {
    pending
  }

  describe("compose") {
    pending
  }

  describe("contains") {

    it("returns true if a StructType contains a StructField") {

      val a = StructField("a", IntegerType, true)
      val s = StructType(
        List(
          a,
          StructField("b", StringType, false),
          StructField("c", StringType, false)
        )
      )
      assert(s.contains(a) === true)

    }

  }

  describe("containsSlice") {
    pending
  }

  describe("#copyToArray") {
    pending
  }

  describe("#copyToBuffer") {
    pending
  }

  describe("#corresponds") {
    pending
  }

  describe("#count") {
    pending
  }

  describe("#defaultSize") {
    pending
  }

  describe("#diff") {

    it("returns the StructFields included in the receiver, but not in the argument") {

      val a = StructField("a", IntegerType, true)
      val b = StructField("b", StringType, false)
      val c = StructField("c", StringType, false)

      val s1 = StructType(List(a, b, c))
      val s2 = StructType(List(a, b))

      assert(s1.diff(s2) === List(c))

    }

    it("works differently when the receiver is switched") {

      val a = StructField("a", IntegerType, true)
      val b = StructField("b", StringType, false)
      val c = StructField("c", StringType, false)

      val s1 = StructType(List(a, b, c))
      val s2 = StructType(List(a, b))

      assert(s2.diff(s1) === List())

    }

  }

  describe("#distinct") {
    pending
  }

  describe("#drop") {
    pending
  }

  describe("#dropRight") {
    pending
  }

  describe("#dropWhile") {
    pending
  }

  describe("#endsWith") {
    pending
  }

  describe("#equals") {

    it("returns true when two StructTypes are equal") {

      val s1 = StructType(
        List(
          StructField("a", IntegerType, true)
        )
      )
      val s2 = StructType(
        List(
          StructField("a", IntegerType, true)
        )
      )
      assert(s1.equals(s2) === true)

    }

    pending
  }

  describe("#exists") {
    pending
  }

  describe("#fieldIndex") {
    pending
  }

  describe("#fieldNames") {
    pending
  }

  describe("#fields") {
    pending
  }

  describe("#filter") {
    pending
  }

  describe("#filterNot") {
    pending
  }

  describe("#find") {
    pending
  }

  describe("#flatMap") {
    pending
  }

  describe("#flatten") {
    pending
  }

  describe("#fold") {
    pending
  }

  describe("#foldLeft") {
    pending
  }

  describe("#foldRight") {
    pending
  }

  describe("#forall") {
    pending
  }

  describe("#foreach") {
    pending
  }

  describe("#genericBuilder") {
    pending
  }

  describe("#groupBy") {
    pending
  }

  describe("#grouped") {
    pending
  }

  describe("#hasDefiniteSize") {
    pending
  }

  describe("#hashCode") {
    pending
  }

  describe("#head") {
    pending
  }

  describe("#headOption") {
    pending
  }

  describe("#indexOf") {
    pending
  }

  describe("#indexOfSlice") {
    pending
  }

  describe("#indexWhere") {
    pending
  }

  describe("#indices") {
    pending
  }

  describe("#init") {
    pending
  }

  describe("#inits") {
    pending
  }

  describe("#intersect") {
    pending
  }

  describe("#isDefinedAt") {
    pending
  }

  describe("#isEmpty") {
    pending
  }

  describe("#isTraversableAgain") {
    pending
  }

  describe("#iterator") {
    pending
  }

  describe("#json") {
    pending
  }

  describe("#last") {
    pending
  }

  describe("#lastIndexOf") {
    pending
  }

  describe("#lastIndexOfSlice") {
    pending
  }

  describe("#lastIndexWhere") {
    pending
  }

  describe("#lastOption") {
    pending
  }

  describe("#length") {
    pending
  }

  describe("#lengthCompare") {
    pending
  }

  describe("#lift") {
    pending
  }

  describe("#map") {
    pending
  }

  describe("#max") {
    pending
  }

  describe("#maxBy") {
    pending
  }

  describe("#min") {
    pending
  }

  describe("#minBy") {
    pending
  }

  describe("#mkString") {
    pending
  }

  describe("#nonEmpty") {
    pending
  }

  describe("#orElse") {
    pending
  }

  describe("#padTo") {
    pending
  }

  describe("#par") {
    pending
  }

  describe("#partition") {
    pending
  }

  describe("#patch") {
    pending
  }

  describe("#permutations") {
    pending
  }

  describe("#prefixLength") {
    pending
  }

  describe("#prettyJson") {
    pending
  }

  describe("#printTreeString") {
    pending
  }

  describe("#product") {
    pending
  }

  describe("#productArity") {
    pending
  }

  describe("#productElement") {
    pending
  }

  describe("#productIterator") {
    pending
  }

  describe("#productPrefix") {
    pending
  }

  describe("#reduce") {
    pending
  }

  describe("#reduceLeft") {
    pending
  }

  describe("#reductLeftOption") {
    pending
  }

  describe("#reduceOption") {
    pending
  }

  describe("#reduceRight") {
    pending
  }

  describe("#reduceRightOption") {
    pending
  }

  describe("#repr") {
    pending
  }

  describe("#reverse") {
    pending
  }

  describe("#reverseIterator") {
    pending
  }

  describe("#reverseMap") {
    pending
  }

  describe("#runWith") {
    pending
  }

  describe("#sameElements") {
    pending
  }

  describe("#scan") {
    pending
  }

  describe("#scanLeft") {
    pending
  }

  describe("#scanRight") {
    pending
  }

  describe("#segmentLength") {
    pending
  }

  describe("#seq") {
    pending
  }

  describe("#simpleString") {
    pending
  }

  describe("#size") {
    pending
  }

  describe("#slice") {
    pending
  }

  describe("#sliding") {
    pending
  }

  describe("#sortBy") {
    pending
  }

  describe("#sorted") {
    pending
  }

  describe("#sortWith") {
    pending
  }

  describe("#span") {
    pending
  }

  describe("#splitAt") {
    pending
  }

  describe("#sql") {
    pending
  }

  describe("#startsWith") {
    pending
  }

  describe("#stringPrefix") {
    pending
  }

  describe("#sum") {
    pending
  }

  describe("#tail") {
    pending
  }

  describe("#tails") {
    pending
  }

  describe("#take") {
    pending
  }

  describe("#takeRight") {
    pending
  }

  describe("#takeWhile") {
    pending
  }

  describe("#to") {
    pending
  }

  describe("#toArray") {
    pending
  }

  describe("#toBuffer") {
    pending
  }

  describe("#toIndexedSeq") {
    pending
  }

  describe("#toIterable") {
    pending
  }

  describe("#toIterator") {
    pending
  }

  describe("#toList") {
    pending
  }

  describe("#toMap") {
    pending
  }

  describe("#toSeq") {
    pending
  }

  describe("#toSet") {
    pending
  }

  describe("#toStream") {
    pending
  }

  describe("#toString") {
    pending
  }

  describe("#toTraversable") {
    pending
  }

  describe("#toVector") {
    pending
  }

  describe("#transpose") {
    pending
  }

  describe("#treeString") {
    pending
  }

  describe("#typeName") {
    pending
  }

  describe("#union") {
    pending
  }

  describe("#unzip") {
    pending
  }

  describe("#unzip3") {
    pending
  }

  describe("#updated") {
    pending
  }

  describe("#view") {
    pending
  }

  describe("#withFilter") {
    pending
  }

  describe("#zip") {
    pending
  }

  describe("#zipAll") {
    pending
  }

  describe("#zipWithIndex") {
    pending
  }

}
