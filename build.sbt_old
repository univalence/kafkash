import sbtassembly.AssemblyPlugin.defaultUniversalScript

name := "kafkash"
scalaVersion := "2.13.6"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients"   % "2.8.0",
  "ch.qos.logback"   % "logback-core"    % "1.2.3",
  "ch.qos.logback"   % "logback-classic" % "1.2.3",
  "org.jline"        % "jline-terminal"  % "3.20.0",
  "org.jline"        % "jline-reader"    % "3.20.0",
  "org.jline"        % "jline-builtins"  % "3.20.0",
  "org.scalatest"   %% "scalatest"       % "3.2.9" % Test
)

scalacOptions ++= Seq(
  "-Wunused"
)

assemblyPrependShellScript := Some(defaultUniversalScript(shebang = false))
assembly / assemblyJarName := s"${name.value}"
assembly / mainClass := Some("io.univalence.kafkash.KafkaShellMain")

ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.5.0"
semanticdbEnabled := true
semanticdbVersion := scalafixSemanticdb.revision

Global / onChangedBuildSource := ReloadOnSourceChanges