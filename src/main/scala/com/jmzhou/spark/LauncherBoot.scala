package com.jmzhou.spark

import java.util

import org.apache.spark.launcher.SparkAppHandle
import org.apache.spark.launcher.SparkLauncher
import mu.atlas.graph.log.LogFactory
import mu.atlas.graph.utils.Props

/**
  * Created by zhoujiamu on 2019/9/5.
  */
class LauncherBoot(prop: Props) {

  private val logger = LogFactory.getLogger(getClass)
  private var appMonitor: SparkAppHandle = _

  def waitForCompleted(): Boolean ={
    var success = false
    try{
      val launcher = initSparkLauncher()
      if (prop.getProperty("log.level").toLowerCase == "debug"){
        val process = launcher.launch()
        new Thread(new JobRunnable(process.getErrorStream)).start()
        new Thread(new JobRunnable(process.getInputStream)).start()
        logger.info("*"*50)
        logger.info("提交spark请求")

        val exitCode = process.waitFor()
        logger.info("请求结果状态码：" + exitCode)

        success = exitCode == 0
      } else {
        appMonitor = launcher.setVerbose(true).startApplication()
        success = applicationMonitor()
      }
    } catch {
      case e: Exception => logger.error(e)
    }
    success
  }


  private def applicationMonitor() = {
    appMonitor.addListener(new SparkAppHandle.Listener() {
      override def stateChanged(handle: SparkAppHandle): Unit = {
        logger.info("*"*50)
        logger.info("State Changed [state=" + handle.getState + "]")
        logger.info("AppId=" + handle.getAppId)
      }
      override def infoChanged(handle: SparkAppHandle): Unit = {}
    })
    while(!isCompleted(appMonitor.getState))
      try {
        Thread.sleep(3000L)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
    val success = appMonitor.getState == SparkAppHandle.State.FINISHED
    success
  }

  private def isCompleted(state: SparkAppHandle.State): Boolean = {
    state match {
      case SparkAppHandle.State.FINISHED => true
      case SparkAppHandle.State.FAILED => true
      case SparkAppHandle.State.KILLED => true
      case SparkAppHandle.State.LOST => true
      case _ => false
    }
  }

  private def initSparkLauncher(): SparkLauncher = {
    logger.info("launcher boot config:\n"+prop.toString)

    val envConfig = new util.HashMap[String, String]

    // 配置hadoop的xml文件本地路径
    envConfig.put("HADOOP_CONF_DIR", prop.getProperty("hadoop.conf.dir"))

    // 配置yarn的xml文件本地路径
    envConfig.put("YARN_CONF_DIR", prop.getProperty("yarn.conf.dir"))

    val launcher = new SparkLauncher(envConfig)

    // 设置算法入口类所在的jar包本地路径
    launcher.setAppResource(prop.getProperty("app.resource"))

    // 设置算法入主类, 同 spark-submit中的 --class 参数
    launcher.setMainClass(prop.getProperty("main.class"))

    launcher.setMaster(prop.getProperty("master"))
    launcher.setDeployMode(prop.getProperty("deploy.mode"))

    // 设置算法依赖的包的本地路径，多个jar包用逗号","隔开，如果是spark on yarn只需要把核心算法包放这里即可，
    // spark相关的依赖包可以预先上传到hdfs并通过 spark.yarn.jars参数指定；
    // 如果是spark standalone则需要把所有依赖的jar全部放在这里
    launcher.addJar(prop.getProperty("jars", ""))

    // 设置应用的名称
    launcher.setAppName(prop.getProperty("app.name"))

    // 设置spark客户端安装包的home目录，提交算法时需要借助bin目录下的spark-submit脚本
    launcher.setSparkHome(prop.getProperty("spark.home"))

    // 设置内存和核数
    launcher.addSparkArg("--driver-memory", prop.getProperty("spark.driver.memory", "4G"))
    launcher.addSparkArg("--driver-cores", prop.getProperty("spark.driver.cores", "2"))
    launcher.addSparkArg("--num-executors", prop.getProperty("spark.num.executors", "20"))
    launcher.addSparkArg("--executor-cores", prop.getProperty("spark.executor.cores", "4"))
    launcher.addSparkArg("--executor-memory", prop.getProperty("spark.executor.memory", "20G"))
//    launcher.addSparkArg("--principal", "work")

    prop.keys().filter(_.startsWith("spark")).foreach(key => {
      val value = prop.getProperty(key)
      launcher.setConf(key, value)
    })

    // 设置算法入口参数
    if (prop.isExist("app.args"))
      launcher.addAppArgs(prop.getProperty("app.args").split(" "): _*)

    launcher

  }

}
