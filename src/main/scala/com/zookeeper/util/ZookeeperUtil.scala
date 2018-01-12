package com.zookeeper.util

import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import org.apache.zookeeper.CreateMode

object ZookeeperUtil {
  val PERSISTENT = CreateMode.PERSISTENT //短暂，持久
  val EPHEMERAL = CreateMode.EPHEMERAL
  def getzkClient(zk: String) = {
    val zkClient = new ZkClient(zk, 10000, 10000, ZKStringSerializer)
    zkClient
  }
  def isExist(
    zkClient: ZkClient,
    path: String) = {
    zkClient.exists(path)
  }

  def createFileOrDir(
    zkClient: ZkClient,
    path: String,
    data: String) = {
    if (!zkClient.exists(path))
      zkClient.create(path, data, PERSISTENT)
    else "is exist"
  }
  def readData(
    zkClient: ZkClient,
    path: String)= {
    zkClient.readData(path).toString()
  }
  def writeData(
    zkClient: ZkClient,
    path: String,
    data: String) = {
    if(!zkClient.exists(path)){
      zkClient.create(path, data, PERSISTENT)
    }
    zkClient.writeData(path, data)
  }
}