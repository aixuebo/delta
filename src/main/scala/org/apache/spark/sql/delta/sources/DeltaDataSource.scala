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

package org.apache.spark.sql.delta.sources

import scala.util.{Failure, Success, Try}

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.DatabricksLogging
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.PartitionUtils
import org.apache.hadoop.fs.Path
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/** A DataSource V1 for integrating Delta into Spark SQL batch and Streaming APIs. */
class DeltaDataSource
  extends RelationProvider
  with StreamSourceProvider
  with StreamSinkProvider
  with CreatableRelationProvider
  with DataSourceRegister
  with DeltaLogging {

  SparkSession.getActiveSession.foreach { spark =>
    // Enable "passPartitionByAsOptions" to support "write.partitionBy(...)"
    // TODO Remove this when upgrading to Spark 3.0.0
    spark.conf.set("spark.sql.legacy.sources.write.passPartitionByAsOptions", "true")
  }


  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    if (schema.nonEmpty) { //实时流该框架不支持定义schema
      throw DeltaErrors.specifySchemaAtReadTimeException
    }
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })

    val (_, maybeTimeTravel) = DeltaTableUtils.extractIfPathContainsTimeTravel(
      sqlContext.sparkSession, path)
    if (maybeTimeTravel.isDefined) throw DeltaErrors.timeTravelNotSupportedException

    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)
    if (deltaLog.snapshot.schema.isEmpty) {
      throw DeltaErrors.schemaNotSetException
    }
    (shortName(), deltaLog.snapshot.schema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    if (schema.nonEmpty) {
      throw DeltaErrors.specifySchemaAtReadTimeException
    }
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)
    if (deltaLog.snapshot.schema.isEmpty) {
      throw DeltaErrors.schemaNotSetException
    }
    val options = new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
    DeltaSource(sqlContext.sparkSession, deltaLog, options)
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Complete) {
      throw DeltaErrors.outputModeNotSupportedException(getClass.getName, outputMode)
    }
    val deltaOptions = new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
    new DeltaSink(sqlContext, new Path(path), partitionColumns, outputMode, deltaOptions)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    //是Set集合类型,表示分区字段集合
    val partitionColumns = parameters.get(DeltaSourceUtils.PARTITIONING_COLUMNS_KEY) //json形式
      .map(DeltaDataSource.decodePartitioningColumns)//把json的内容转换成set集合,即json内容是数组形式[]
      .getOrElse(Nil)

    //事务日志对象
    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)
    WriteIntoDelta(
      deltaLog = deltaLog,
      mode = mode,
      new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf),
      partitionColumns = partitionColumns,
      configuration = Map.empty,
      data = data).run(sqlContext.sparkSession)

    deltaLog.createRelation()
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val maybePath = parameters.getOrElse("path", {//读取表路径
      throw DeltaErrors.pathNotSpecifiedException
    })

    // Log any invalid options that are being passed in
    DeltaOptions.verifyOptions(CaseInsensitiveMap(parameters))

    val timeTravelByParams = DeltaDataSource.getTimeTravelVersion(parameters) //确定查询哪个版本数据
    // TODO(burak): Move all this logic into DeltaTableV2 when Spark 3.0 is ready
    // Handle time travel  返回值path/_delta_log目录、读取的分区信息、版本信息
    val (path, partitionFilters, timeTravelByPath) =
      DeltaDataSource.parsePathIdentifier(sqlContext.sparkSession, maybePath)

    if (timeTravelByParams.isDefined && timeTravelByPath.isDefined) {
      throw DeltaErrors.multipleTimeTravelSyntaxUsed
    }

    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)

    //找到分区表达式集合
    val partitionPredicates =
      DeltaDataSource.verifyAndCreatePartitionFilters(maybePath, deltaLog, partitionFilters)

    deltaLog.createRelation(partitionPredicates, timeTravelByParams.orElse(timeTravelByPath))
  }

  override def shortName(): String = {
    DeltaSourceUtils.ALT_NAME
  }


}

object DeltaDataSource extends DatabricksLogging {
  private implicit val formats = Serialization.formats(NoTypeHints)

  final val TIME_TRAVEL_SOURCE_KEY = "__time_travel_source__"

  /**
   * The option key for time traveling using a timestamp. The timestamp should be a valid
   * timestamp string which can be cast to a timestamp type.
   */
  final val TIME_TRAVEL_TIMESTAMP_KEY = "timestampAsOf"

  /**
   * The option key for time traveling using a version of a table. This value should be
   * castable to a long.
   */
  final val TIME_TRAVEL_VERSION_KEY = "versionAsOf"

  def encodePartitioningColumns(columns: Seq[String]): String = {
    Serialization.write(columns)
  }

  //把json的内容转换成set集合,即json内容是数组形式[]
  def decodePartitioningColumns(str: String): Seq[String] = {
    Serialization.read[Seq[String]](str)
  }

  /**
   * Extract the Delta path if `dataset` is created to load a Delta table. Otherwise returns `None`.
   * Table UI in universe will call this.
   */
  def extractDeltaPath(dataset: Dataset[_]): Option[String] = {
    if (dataset.isStreaming) {
      dataset.queryExecution.logical match {
        case logical: org.apache.spark.sql.execution.streaming.StreamingRelation =>
          if (logical.dataSource.providingClass == classOf[DeltaDataSource]) {
            CaseInsensitiveMap(logical.dataSource.options).get("path")
          } else {
            None
          }
        case _ => None
      }
    } else {
      dataset.queryExecution.analyzed match {
        case DeltaTable(tahoeFileIndex) =>
          Some(tahoeFileIndex.path.toString)
        case SubqueryAlias(_, DeltaTable(tahoeFileIndex)) =>
          Some(tahoeFileIndex.path.toString)
        case _ => None
      }
    }
  }

  /**
   * For Delta, we allow certain magic to be performed through the paths that are provided by users.
   * Normally, a user specified path should point to the root of a Delta table. However, some users
   * are used to providing specific partition values through the path, because of how expensive it
   * was to perform partition discovery before. We treat these partition values as logical partition
   * filters, if a table does not exist at the provided path.
   *
   * In addition, we allow users to provide time travel specifications through the path. This is
   * provided after an `@` symbol after a path followed by a time specification in
   * `yyyyMMddHHmmssSSS` format, or a version number preceded by a `v`.
   *
   * This method parses these specifications and returns these modifiers only if a path does not
   * really exist at the provided path. We first parse out the time travel specification, and then
   * the partition filters. For example, a path specified as:
   *      /some/path/partition=1@v1234
   * will be parsed into `/some/path` with filters `partition=1` and a time travel spec of version
   * 1234.
   *
   * @return A tuple of the root path of the Delta table, partition filters, and time travel options
    *返回值path/_delta_log目录、读取的分区信息、版本信息
   */
  def parsePathIdentifier(
      spark: SparkSession,
      userPath: String): (Path, Seq[(String, String)], Option[DeltaTimeTravelSpec]) = {
    // Handle time travel
    val (path, timeTravelByPath) = DeltaTableUtils.extractIfPathContainsTimeTravel(spark, userPath) //抽取真实路径 和 版本信息

    val hadoopPath = new Path(path)
    //rootPath = path/_delta_log目录
    val rootPath = DeltaTableUtils.findDeltaTableRoot(spark, hadoopPath).getOrElse {//找到path/_delta_log目录
      val fs = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
      if (!fs.exists(hadoopPath)) { //说明path/_delta_log目录不存在
        throw DeltaErrors.pathNotExistsException(path)
      }
      hadoopPath
    }

    val partitionFilters = if (rootPath != hadoopPath) {
      logConsole(
        """
          |WARNING: loading partitions directly with delta is not recommended.
          |If you are trying to read a specific partition, use a where predicate.
          |
          |CORRECT: spark.read.format("delta").load("/data").where("part=1")
          |INCORRECT: spark.read.format("delta").load("/data/part=1")
        """.stripMargin)

      val fragment = hadoopPath.toString.substring(rootPath.toString.length() + 1)
      try {
        PartitionUtils.parsePathFragmentAsSeq(fragment) //fragment eg:fieldOne=1/fieldTwo=2  返回分区的key与value集合
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          throw DeltaErrors.partitionPathParseException(fragment)
      }
    } else {
      Nil
    }

    //返回值path/_delta_log目录、读取的分区信息、版本信息
    (rootPath, partitionFilters, timeTravelByPath)
  }

  /**
   * Verifies that the provided partition filters are valid and returns the corresponding
   * expressions.
    * 返回计算分区列的表达式集合
    * 校验分区列是否合法,以及分区列是否有匹配的文件存在
   */
  def verifyAndCreatePartitionFilters(
      userPath: String,
      deltaLog: DeltaLog,
      partitionFilters: Seq[(String, String)]): Seq[Expression] = {
    if (partitionFilters.nonEmpty) {
      val snapshot = deltaLog.update()
      val metadata = snapshot.metadata

      val badColumns = partitionFilters.map(_._1).filterNot(metadata.partitionColumns.contains) //找到不是分区的列集合
      if (badColumns.nonEmpty) {
        val fragment = partitionFilters.map(f => s"${f._1}=${f._2}").mkString("/")
        throw DeltaErrors.partitionPathInvolvesNonPartitionColumnException(badColumns, fragment) //提示存在不是分区的列信息
      }

      val filters = partitionFilters.map { case (key, value) =>
        // Nested fields cannot be partitions, so we pass the key as a identifier
        EqualTo(UnresolvedAttribute(Seq(key)), Literal(value))
      }
      val files = DeltaLog.filterFileList(
        metadata.partitionSchema, snapshot.allFiles.toDF(), filters)
      if (files.count() == 0) {
        throw DeltaErrors.pathNotExistsException(userPath) //查看分区的文件集合
      }
      filters
    } else {
      Nil
    }
  }

  /** Extracts whether users provided the option to time travel a relation.
    * 通过参数抽取出如何回溯数据
    **/
  def getTimeTravelVersion(parameters: Map[String, String]): Option[DeltaTimeTravelSpec] = {
    val caseInsensitive = CaseInsensitiveMap[String](parameters)
    val tsOpt = caseInsensitive.get(DeltaDataSource.TIME_TRAVEL_TIMESTAMP_KEY) //时间戳回溯
    val versionOpt = caseInsensitive.get(DeltaDataSource.TIME_TRAVEL_VERSION_KEY) //版本号回溯
    val sourceOpt = caseInsensitive.get(DeltaDataSource.TIME_TRAVEL_SOURCE_KEY)

    if (tsOpt.isDefined && versionOpt.isDefined) {//不能既支持时间戳回溯版本，又版本号回溯版本
      throw DeltaErrors.provideOneOfInTimeTravel //抛异常
    } else if (tsOpt.isDefined) {
      Some(DeltaTimeTravelSpec(Some(Literal(tsOpt.get)), None, sourceOpt.orElse(Some("dfReader"))))
    } else if (versionOpt.isDefined) {
      val version = Try(versionOpt.get.toLong) match { //版本号是否是整数
        case Success(v) => v
        case Failure(t) => throw new IllegalArgumentException(
          s"${DeltaDataSource.TIME_TRAVEL_VERSION_KEY} needs to be a valid bigint value.", t) //必须是一个有效的整数
      }
      Some(DeltaTimeTravelSpec(None, Some(version), sourceOpt.orElse(Some("dfReader"))))
    } else {
      None
    }
  }
}
