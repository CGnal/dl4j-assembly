import de.heikoseeberger.sbtheader.license.Apache2_0
import sbt._

val sparkVersion = "1.6.0-cdh5.8.2"

val hadoopVersion = "2.6.0-cdh5.8.2"

val nd4jVersion = "0.6.0"

val dl4jVersion = "0.6.0"

val datavecVersion = "0.6.0"

val scalaTestVersion = "3.0.0"

organization := "com.cgnal.dl4j"

name := "dl4j-assembly"

version in ThisBuild := s"$dl4jVersion"

val assemblyName = s"dl4j-assembly"

scalaVersion := "2.10.6"

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

scalariformSettings

scalastyleFailOnError := true

dependencyUpdatesExclusions := moduleFilter(organization = "org.scala-lang")

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture"
)

wartremoverErrors ++= Seq(
  Wart.Any,
  Wart.Any2StringAdd,
  Wart.AsInstanceOf,
  Wart.DefaultArguments,
  Wart.EitherProjectionPartial,
  Wart.Enumeration,
  Wart.Equals,
  Wart.ExplicitImplicitTypes,
  Wart.FinalCaseClass,
  Wart.FinalVal,
  Wart.ImplicitConversion,
  Wart.IsInstanceOf,
  Wart.JavaConversions,
  Wart.LeakingSealed,
  Wart.ListOps,
  Wart.MutableDataStructures,
  //Wart.NoNeedForMonad,
  Wart.NonUnitStatements,
  Wart.Nothing,
  Wart.Null,
  Wart.Option2Iterable,
  Wart.OptionPartial,
  Wart.Overloading,
  Wart.Product,
  Wart.Return,
  Wart.Serializable,
  Wart.Throw,
  Wart.ToString,
  Wart.TryPartial,
  Wart.Var,
  Wart.While
)

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

val isALibrary = false //this is a library project

val sparkExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.apache.hadoop", "hadoop-client").
    exclude("org.apache.hadoop", "hadoop-yarn-client").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.apache.hadoop", "hadoop-yarn-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")

val assemblyDependencies = (scope: String) => Seq(
  sparkExcludes("org.nd4j" % "nd4j-native-platform" % nd4jVersion % scope)
    exclude("com.fasterxml.jackson.core", "jackson-annotations")
    exclude("com.fasterxml.jackson.core", "jackson-core")
    exclude("com.fasterxml.jackson.core", "jackson-databind")
    exclude("com.fasterxml.jackson.dataformat", "jackson-dataformat-yaml"),
  sparkExcludes("org.deeplearning4j" %% "dl4j-spark" % dl4jVersion % scope)
    exclude("org.apache.spark", "*")
    exclude("com.fasterxml.jackson.core", "jackson-annotations")
    exclude("com.fasterxml.jackson.core", "jackson-core")
    exclude("com.fasterxml.jackson.core", "jackson-databind")
    exclude("com.fasterxml.jackson.dataformat", "jackson-dataformat-yaml"),
  sparkExcludes("org.nd4j" %% "nd4j-kryo" % nd4jVersion % scope)
    exclude("com.esotericsoftware.kryo", "kryo"),
  sparkExcludes("org.datavec" % "datavec-api" % datavecVersion % scope),
  sparkExcludes("org.datavec" %% "datavec-spark" % datavecVersion % scope)
    exclude("org.apache.spark", "*"),
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.4.4" % scope,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.4.4" % scope,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4" % scope,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.4.4" % scope
)

val hadoopClientExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.slf4j", "slf4j-api").
    exclude("javax.servlet", "servlet-api")

/*if it's a library the scope is "compile" since we want the transitive dependencies on the library
  otherwise we set up the scope to "provided" because those dependencies will be assembled in the "assembly"*/
lazy val assemblyDependenciesScope: String = if (isALibrary) "compile" else "provided"

lazy val hadoopDependenciesScope = if (isALibrary) "provided" else "compile"

libraryDependencies ++= Seq(
  sparkExcludes("org.apache.spark" %% "spark-core" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-sql" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-yarn" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-mllib" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-applications-distributedshell" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion % hadoopDependenciesScope)
) ++ assemblyDependencies(assemblyDependenciesScope)

//http://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath/21803413#21803413
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

//http://stackoverflow.com/questions/27824281/sparksql-missingrequirementerror-when-registering-table
fork := true

parallelExecution in Test := false

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(Defaults.itSettings: _*).
  settings(
    libraryDependencies += "org.scalatest" % "scalatest_2.10" % scalaTestVersion % "it,test",
    headers := Map(
      "sbt" -> Apache2_0("2016", "CGnal S.p.A."),
      "scala" -> Apache2_0("2016", "CGnal S.p.A."),
      "conf" -> Apache2_0("2016", "CGnal S.p.A.", "#"),
      "properties" -> Apache2_0("2016", "CGnal S.p.A.", "#")
    )
  ).
  enablePlugins(AutomateHeaderPlugin).
  enablePlugins(JavaAppPackaging).
  disablePlugins(AssemblyPlugin)

lazy val projectAssembly = (project in file("assembly")).
  settings(
    ivyScala := ivyScala.value map {
      _.copy(overrideScalaVersion = true)
    },
    assemblyMergeStrategy in assembly := {
      case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyJarName in assembly := s"$assemblyName-${version.value}.jar",
    libraryDependencies ++= assemblyDependencies("compile")
  ) dependsOn root settings (
  projectDependencies := {
    Seq(
      (projectID in root).value.excludeAll(ExclusionRule(organization = "org.apache.spark"),
        if (!isALibrary) ExclusionRule(organization = "org.apache.hadoop") else ExclusionRule())
    )
  })

mappings in Universal := {
  val universalMappings = (mappings in Universal).value
  val filtered = universalMappings filter {
    case (f, n) =>
      !n.endsWith(s"${organization.value}.${name.value}-${version.value}.jar")
  }
  val fatJar: File = new File(s"${System.getProperty("user.dir")}/assembly/target/scala-2.10/$assemblyName-${version.value}.jar")
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

scriptClasspath ++= Seq(s"$assemblyName-${version.value}.jar")
