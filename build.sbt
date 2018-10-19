import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(SpacesAroundMultiImports, false)
  .setPreference(DanglingCloseParenthesis, Force)

resolvers += "jitpack" at "https://jitpack.io"

name := "spark-spec"

version := "0.0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided"

libraryDependencies += "mrpowers" % "spark-daria" % "2.3.1_0.25.0" % "test"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "2.3.1_0.15.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

fork in Test := true
envVars in Test := Map("PROJECT_ENV" -> "test")
javaOptions ++= Seq("-Xms1g", "-Xmx2g", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")