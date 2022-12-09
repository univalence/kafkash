package io.univalence.kafkash.command

enum Command(commandType: CommandType) {
  case Quit extends Command(CommandType.Quit)
  case Help(command: Option[CommandType]) extends Command(CommandType.Help)
  case ShowTopic(topic: String) extends Command(CommandType.ShowTopic)
  case ShowTopics extends Command(CommandType.ShowTopics)
  case ShowGroup(group: String) extends Command(CommandType.ShowGroup)
  case ShowGroups extends Command(CommandType.ShowGroups)
  case ShowCluster extends Command(CommandType.ShowCluster)
  case DeleteTopic(name: String) extends Command(CommandType.DeleteTopic)
  case DeleteGroup(name: String) extends Command(CommandType.DeleteGroup)
  case CreateGroup(name: String, topic: String) extends Command(CommandType.CreateGroup)
  case CreateTopic(name: String, partitions: Option[Int], replicas: Option[Int])
      extends Command(CommandType.CreateTopic)
  case Select(fromTopic: String, format: String, last: Long) extends Command(CommandType.Select)
  case SelectFollow(fromTopic: String, format: String) extends Command(CommandType.Select)
  case Insert(toTopic: String, key: String, value: String) extends Command(CommandType.Insert)
}
