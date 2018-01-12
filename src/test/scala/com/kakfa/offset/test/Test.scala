package com.kakfa.offset.test
import com.zookeeper.util.KafkaUtil
import com.zookeeper.util.ZookeeperUtil
import java.util.Date
import java.text.SimpleDateFormat
object Test {
val smp = new SimpleDateFormat("yyyyMMdd")
//val zk="solr1,solr2,mongodb3"
val zk="zk1,zk2,zk3"
val broker="kafka1,kafka2,kafka3"
  def main(args: Array[String]): Unit = {
    val groupid="kafkatopicoffset"
    recordKafkaoffset(groupid)
  }
  def recordKafkaoffset(groupid:String) {
    var kafkaParams = Map[String, String](
      "metadata.broker.list" ->broker ,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> groupid)
    val zkClient = ZookeeperUtil.getzkClient(zk)
    val date = new Date()
    val day = smp.format(date)
    val util = new KafkaUtil(kafkaParams)
    val alltopics = util.getAlltopics(zkClient).toSet
    val topicpart = util.getLatestLeaderOffsets(alltopics, zkClient)
    //将不同的topic写入zookeeper /consumers/alltopicoffset/offsets
    //更新时间写在这个下面         /consumers/alltopicoffset/time/
    val parentPath = s"""/consumers/${groupid}"""
    val offsetPath = s"""${parentPath}/offsets"""
    val dayPath = s"""${offsetPath}/${day}"""
    println(ZookeeperUtil.createFileOrDir(zkClient, parentPath, date.toString()))
    println(ZookeeperUtil.createFileOrDir(zkClient, offsetPath, date.toString()))
    println(ZookeeperUtil.createFileOrDir(zkClient, dayPath, date.toString()))
    topicpart
      .map { case (tp, offset) => (tp.topic, tp.partition, offset) }
      .toList
      .groupBy(_._1)
      .foreach {case (topic, list) =>
          val topicPath = s"""${dayPath}/${topic}"""
          println(ZookeeperUtil.createFileOrDir(zkClient, topicPath, ""))
          list.foreach {
            case (topic, part, offset) =>
              val path = s"""${topicPath}/${part}"""
              ZookeeperUtil.writeData(zkClient, path, offset.toString)
          }
      }

  }
}