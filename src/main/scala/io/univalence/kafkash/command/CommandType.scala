package io.univalence.kafkash.command

enum CommandType(val commandName: String, val usage: String, val description: String) {
  case Quit
    extends CommandType(
      commandName = "QUIT",
      usage       = "QUIT",
      description = "Quit application."
    )
  case Help
    extends CommandType(
      commandName = "HELP",
      usage       = "HELP [<command>]",
      description = "Display this help."
    )
  case ShowTopic
    extends CommandType(
      commandName = "SHOW TOPIC",
      usage       = "SHOW TOPIC <topic>",
      description = "Show informations about <topic>."
    )
  case ShowTopics
    extends CommandType(
      commandName = "SHOW TOPICS",
      usage       = "SHOW TOPICS",
      description = "Show topic list."
    )
  case ShowGroup
    extends CommandType(
      commandName = "SHOW GROUP",
      usage       = "SHOW GROUP <consumer_group>",
      description = "Show informations about <consumer_group>."
    )
  case ShowGroups
    extends CommandType(
      commandName = "SHOW GROUPS",
      usage       = "SHOW GROUPS",
      description = "Show group list."
    )
  case ShowCluster
    extends CommandType(
      commandName = "SHOW CLUSTER",
      usage       = "SHOW CLUSTER",
      description = "Show cluster information."
    )
  case CreateGroup
    extends CommandType(
      commandName = "CREATE GROUP",
      usage       = "CREATE GROUP <group> [FOR <topic>]",
      description = "Create a consumer group."
    )
  case CreateTopic
    extends CommandType(
      commandName = "CREATE TOPIC",
      usage       = "CREATE TOPIC <topic> [PARTITIONS: <int>] [REPLICAS: <int>]",
      description = "Create a topic (default: partitions=3, replicas=number of cluster nodes)."
    )
  case DeleteTopic
    extends CommandType(
      commandName = "DELETE TOPIC",
      usage       = "DELETE TOPIC <topic>",
      description = "Delete a topic."
    )
  case DeleteGroup
    extends CommandType(
      commandName = "DELETE GROUP",
      usage       = "DELETE GROUP <group>",
      description = "Delete a consumer group."
    )
  case Select
      extends CommandType(
        commandName = "SELECT",
        usage       = "SELECT FROM <topic> (LAST [<n>] | FOLLOW)",
        description = "Read data from topic. FOLLOW parameter follows new messages until you hit <Ctrl+C>."
      )
  case Insert
    extends CommandType(
      commandName = "INSERT",
      usage       = "INSERT INTO <topic> KEY <key> VALUE <value>",
      description = "Push data into topic."
    )
}

