// The simplest possible sbt build file is just one line:

scalaVersion := "2.13.3"

scalacOptions ++= Seq("-unchecked", "-deprecation")

name := "streams"
organization := "ca.uvic"
version := "1.0"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"

