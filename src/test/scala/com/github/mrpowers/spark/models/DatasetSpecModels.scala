package com.github.mrpowers.spark.models

case class UnionAnyInput(name: String, age: Int, response: Option[String])
case class UnionAnyOutput(name: String, age: Int, response: Option[String])
case class Person(name: String)
case class Student(student: String)
case class PersonWithAge(name: String, age: Int)
