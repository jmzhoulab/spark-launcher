package com.jmzhou.spark

import mu.atlas.graph.utils.Props

/**
  * Created by zhoujiamu on 2019/9/5.
  */
object LauncherTest {

  def main(args: Array[String]): Unit = {

    val prop = new Props("conf/launcher.properties")

    System.setProperty("HADOOP_USER_NAME", "work")
    System.setProperty("USER", "work")

    val boot = new LauncherBoot(prop)

    boot.waitForCompleted()

  }

}
