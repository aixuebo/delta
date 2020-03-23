/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.storage

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * Default implementation of [[LogStore]] for Hadoop [[FileSystem]] implementations.
 */
abstract class HadoopFileSystemLogStore(
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends LogStore {

  def this(sc: SparkContext) = this(sc.getConf, sc.hadoopConfiguration)

  protected def getHadoopConfiguration: Configuration = {
    SparkSession.getActiveSession.map(_.sessionState.newHadoopConf()).getOrElse(hadoopConf)
  }

  //路径下每一行内容是一个元素
  override def read(path: Path): Seq[String] = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    val stream = fs.open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
      IOUtils.readLines(reader).asScala.map(_.trim)
    } finally {
      stream.close()
    }
  }

  //获取目录下所有子子孙孙文件
  override def listFrom(path: Path): Iterator[FileStatus] = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    val files = fs.listStatus(path.getParent)
    files.filter(_.getPath.getName >= path.getName).sortBy(_.getPath.getName).iterator
  }

  override def resolvePathOnPhysicalStorage(path: Path): Path = {
    path.getFileSystem(getHadoopConfiguration).makeQualified(path)
  }

  /**
   * An internal write implementation that uses FileSystem.rename().
   *
   * This implementation should only be used for the underlying file systems that support atomic
   * renames, e.g., Azure is OK but HDFS is not.
   */
  protected def writeWithRename(
      path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val fs = path.getFileSystem(getHadoopConfiguration)

    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    if (overwrite) {
      val stream = fs.create(path, true)
      try {
        actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write) //对action内容,转换成字节数组,输出到文件流中
      } finally {
        stream.close()
      }
    } else {
      if (fs.exists(path)) { //不允许该路径存在
        throw new FileAlreadyExistsException(path.toString)
      }

      //先写入到临时文件
      val tempPath = createTempPath(path)
      var streamClosed = false // This flag is to avoid double close
      var renameDone = false // This flag is to save the delete operation in most of cases.
      val stream = fs.create(tempPath)
      try {
        actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
        stream.close()
        streamClosed = true //true表示临时文件写入完成
        try {
          if (fs.rename(tempPath, path)) {
            renameDone = true//表示文件rename完成
          } else {
            if (fs.exists(path)) {
              throw new FileAlreadyExistsException(path.toString) //应该不会走到这里来,因为开始的时候已经做了校验
            } else {
              throw new IllegalStateException(s"Cannot rename $tempPath to $path")
            }
          }
        } catch {
          case _: org.apache.hadoop.fs.FileAlreadyExistsException =>
            throw new FileAlreadyExistsException(path.toString)
        }
      } finally {
        if (!streamClosed) {
          stream.close()
        }
        if (!renameDone) {
          fs.delete(tempPath, false) //删除临时文件
        }
      }
    }
  }

  //创建临时文件
  protected def createTempPath(path: Path): Path = {
    new Path(path.getParent, s".${path.getName}.${UUID.randomUUID}.tmp")
  }

  override def invalidateCache(): Unit = {}
}
