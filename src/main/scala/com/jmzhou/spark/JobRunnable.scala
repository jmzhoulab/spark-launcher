package com.jmzhou.spark

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}

/**
  * Created by zhoujiamu on 2019/9/5.
  */
class JobRunnable() extends Runnable{

  private final var reader: BufferedReader = _

  def this(reader: BufferedReader) {
    this()
    this.reader = reader
  }

  def this(inputStream: InputStream) {
    this(new BufferedReader(new InputStreamReader(inputStream)))
  }

  override def run(): Unit = {
    var line: String = null
    try {
      line = reader.readLine()
      while ( {
        line != null
      }) {
        System.out.println(line)
        line = reader.readLine
      }
      reader.close()
    } catch {
      case e: IOException =>
        System.out.println("There is a exception when getting log: " + e)
    }
  }

}
