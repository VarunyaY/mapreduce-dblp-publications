name := "Varunya_Yanamadala_hw2"

version := "0.1"

scalaVersion := "2.13.1"

//https://stackoverflow.com/questions/25144484/sbt-assembly-deduplication-found-error
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions := Seq("-target:jvm-1.8")

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.6.4"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.0" exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.0" exclude("org.slf4j", "slf4j-log4j12")
//libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M2"

mainClass in (Compile, run) := Some("com.vyanam3.hw2.MRDriver")
mainClass in assembly := Some("com.vyanam3.hw2.MRDriver")

assemblyJarName in assembly := "varunya_yanamadala_hw2.jar"
