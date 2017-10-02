import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(SpacesAroundMultiImports, false)
  .setPreference(DanglingCloseParenthesis, Force)

name := "spark-spec"

spName := "mrpowers/spark-spec"

spShortDescription := "Spark spec"

spDescription := "Test suite for the behavior of Spark"

version := "0.0.1"

scalaVersion := "2.11.8"
sparkVersion := "2.2.0"

libraryDependencies += "mrpowers" % "spark-daria" % "2.2.0_0.12.0"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test"

libraryDependencies += "org.apache.commons" % "commons-text" % "1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

sparkComponents ++= Seq("sql", "mllib")

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true
javaOptions ++= Seq("-Xms4g", "-Xmx8g", "-XX:+CMSClassUnloadingEnabled")
