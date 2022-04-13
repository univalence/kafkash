import sbtassembly.AssemblyPlugin.defaultUniversalScript

lazy val root =
  (project in file("."))
    .enablePlugins(BuildInfoPlugin)
    .settings(
      name             := "kafkash",
      version          := "0.1",
      scalaVersion     := "3.1.1",
      buildInfoKeys    := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
      buildInfoPackage := "io.univalence.kafkash",
      libraryDependencies ++= Seq(
        "dev.zio"                %% "zio"                      % libVersion.zio,
        "dev.zio"                %% "zio-streams"              % libVersion.zio,
        "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1",
        "org.apache.kafka"        % "kafka-clients"            % "2.8.1",
        "ch.qos.logback"          % "logback-core"             % libVersion.logback,
        "ch.qos.logback"          % "logback-classic"          % libVersion.logback,
        "org.jline"               % "jline-terminal"           % libVersion.jline,
        "org.jline"               % "jline-reader"             % libVersion.jline,
        "org.jline"               % "jline-builtins"           % libVersion.jline,
        "org.testcontainers"      % "testcontainers"           % libVersion.testcontainers % Test,
        "org.testcontainers"      % "kafka"                    % libVersion.testcontainers % Test,
        "dev.zio"                %% "zio-test"                 % libVersion.zio            % Test,
        "org.scalatest"          %% "scalatest"                % "3.2.11"                  % Test
      ),
      assemblyPrependShellScript := Some(defaultUniversalScript(shebang = false)),
      assembly / assemblyJarName := s"${name.value}",
      assembly / mainClass       := Some("io.univalence.kafkash.KafkaShApp")
    )

lazy val libVersion =
  new {
    val jline          = "3.21.0"
    val logback        = "1.2.11"
    val testcontainers = "1.16.3"
    val zio            = "2.0.0-RC5"
  }
