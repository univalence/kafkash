name := "kafkash"

version := "0.1"

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
