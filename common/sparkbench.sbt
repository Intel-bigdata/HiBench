import AssemblyKeys._

assemblySettings

name := "SparkBench"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion = "1.1.0"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core"  % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"   % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive"  % sparkVersion % "provided",
    "org.apache.mahout" % "mahout-core" % "0.9",
    "org.apache.mahout" % "mahout-math" % "0.9",
    "com.github.scopt" %% "scopt" % "3.2.0")


resolvers ++= Seq("Apache Repository" at "https://repository.apache.org/content/repositories/releases",
	           "Akka Repository" at "http://repo.akka.io/releases/",
		   "Local Repo" at Path.userHome.asFile.toURI.toURL + "/.m2/repository",
		   Resolver.mavenLocal
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
    {
        case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
        case PathList("javax", "transaction", xs @ _*)     => MergeStrategy.first
        case PathList("javax", "mail", xs @ _*)            => MergeStrategy.first
        case PathList("javax", "activation", xs @ _*)      => MergeStrategy.first
        case PathList("org", "xmlpull", xs @ _*)      => MergeStrategy.first
        case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
        case "application.conf" => MergeStrategy.concat
        case "unwanted.txt"     => MergeStrategy.discard
        case x => old(x)
        }
    }

org.scalastyle.sbt.ScalastylePlugin.Settings
