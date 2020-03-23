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

package com.databricks.spark.util

/**
 * This file contains stub implementation for logging that exists in Databricks.
 */


class TagDefinition

object TagDefinitions {
  object TAG_TAHOE_PATH extends TagDefinition //日志文件的父路径
  object TAG_TAHOE_ID extends TagDefinition //deltaLog.snapshot.metadata.id
  object TAG_ASYNC extends TagDefinition //异步
  object TAG_LOG_STORE_CLASS extends TagDefinition //存储日志的类型
  object TAG_OP_TYPE extends TagDefinition //类型
}

case class OpType(typeName: String, description: String)

class MetricDefinition //表示记录是否成功/失败
object MetricDefinitions {
  object EVENT_LOGGING_FAILURE extends MetricDefinition //记录失败
  object EVENT_TAHOE extends MetricDefinition //记录成功
}

trait DatabricksLogging {
  // scalastyle:off println
  def logConsole(line: String): Unit = println(line)
  // scalastyle:on println

  def recordUsage(
      metric: MetricDefinition,
      quantity: Double,
      additionalTags: Map[TagDefinition, String] = Map.empty,
      blob: String = null,
      forceSample: Boolean = false,
      trimBlob: Boolean = true,
      silent: Boolean = false): Unit = {

  }

  def recordEvent(
      metric: MetricDefinition,
      additionalTags: Map[TagDefinition, String] = Map.empty,
      blob: String = null,//配置json字符串
      trimBlob: Boolean = true): Unit = {
    recordUsage(metric, 1, additionalTags, blob, trimBlob)
  }

  def recordOperation[S](
      opType: OpType,
      opTarget: String = null,
      extraTags: Map[TagDefinition, String],
      isSynchronous: Boolean = true,
      alwaysRecordStats: Boolean = false,
      allowAuthTags: Boolean = false,
      killJvmIfStuck: Boolean = false,
      outputMetric: MetricDefinition = null,
      silent: Boolean = true)(thunk: => S): S = {
    thunk
  }
}
