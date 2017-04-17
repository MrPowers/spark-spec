import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(SpacesAroundMultiImports, false)

name := "spark-spec"

spName := "mrpowers/spark-spec"

spShortDescription := "Spark spec"

spDescription := "Test suite for the behavior of Spark"

version := "0.0.1"

scalaVersion := "2.11.8"
sparkVersion := "2.1.0"

spDependencies += "MrPowers/spark-fast-tests:0.2.0"

sparkComponents ++= Seq("sql", "mllib")

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")