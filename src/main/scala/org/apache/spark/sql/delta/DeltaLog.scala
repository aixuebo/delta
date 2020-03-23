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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.io.{File, FileNotFoundException, IOException}
import java.util.concurrent.{Callable, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

import com.databricks.spark.util.TagDefinitions._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStoreProvider
import com.google.common.cache.{CacheBuilder, RemovalListener, RemovalNotification}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, In, InSet, Literal}
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * Used to query the current state of the log as well as modify it by adding
 * new atomic collections of actions.
 *
 * Internally, this class implements an optimistic concurrency control
 * algorithm to handle multiple readers or writers. Any single read
 * is guaranteed to see a consistent snapshot of the table.
  * 表示delta管理的一张表
 */
class DeltaLog private(
    val logPath: Path,//root/_delta_log目录
    val dataPath: Path,//root目录
    val clock: Clock)
  extends Checkpoints
  with MetadataCleanup
  with LogStoreProvider //log的存储,用于如何存储log内容,以及存储的路径
  with SnapshotManagement
  with ReadChecksum {

  import org.apache.spark.sql.delta.util.FileNames._


  private lazy implicit val _clock = clock

  @volatile private[delta] var asyncUpdateTask: Future[Unit] = _

  protected def spark = SparkSession.active

  /** Used to read and write physical log files and checkpoints. */
  lazy val store = createLogStore(spark)
  /** Direct access to the underlying storage system. */
  private[delta] lazy val fs = logPath.getFileSystem(spark.sessionState.newHadoopConf)

  /** Use ReentrantLock to allow us to call `lockInterruptibly` */
  protected val deltaLogLock = new ReentrantLock()

  /** Delta History Manager containing version and commit history. 如何查询历史提交版本信息*/
  lazy val history = new DeltaHistoryManager(
    this, spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_HISTORY_PAR_SEARCH_THRESHOLD))

  /* --------------- *
   |  Configuration  |
   * --------------- */

  /** Returns the checkpoint interval for this log. Not transactional. */
  def checkpointInterval: Int = DeltaConfigs.CHECKPOINT_INTERVAL.fromMetaData(metadata)

  /**
   * The max lineage length of a Snapshot before Delta forces to build a Snapshot from scratch.
   * Delta will build a Snapshot on top of the previous one if it doesn't see a checkpoint.
   * However, there is a race condition that when two writers are writing at the same time,
   * a writer may fail to pick up checkpoints written by another one, and the lineage will grow
   * and finally cause StackOverflowError. Hence we have to force to build a Snapshot from scratch
   * when the lineage length is too large to avoid hitting StackOverflowError.
   */
  def maxSnapshotLineageLength: Int =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_MAX_SNAPSHOT_LINEAGE_LENGTH)

  /** How long to keep around logically deleted files before physically deleting them. */
  private[delta] def tombstoneRetentionMillis: Long =
    DeltaConfigs.getMilliSeconds(DeltaConfigs.TOMBSTONE_RETENTION.fromMetaData(metadata))

  // TODO: There is a race here where files could get dropped when increasing the
  // retention interval...
  protected def metadata = if (snapshot == null) Metadata() else snapshot.metadata

  /**
   * Tombstones before this timestamp will be dropped from the state and the files can be
   * garbage collected.
   */
  def minFileRetentionTimestamp: Long = clock.getTimeMillis() - tombstoneRetentionMillis

  /**
   * Checks whether this table only accepts appends. If so it will throw an error in operations that
   * can remove data such as DELETE/UPDATE/MERGE.
   */
  def assertRemovable(): Unit = {
    if (DeltaConfigs.IS_APPEND_ONLY.fromMetaData(metadata)) { //表是否只能追加，不能被删除操作
      throw DeltaErrors.modifyAppendOnlyTableException
    }
  }

  /** The unique identifier for this table. */
  def tableId: String = metadata.id

  /**
   * Combines the tableId with the path of the table to ensure uniqueness. Normally `tableId`
   * should be globally unique, but nothing stops users from copying a Delta table directly to
   * a separate location, where the transaction log is copied directly, causing the tableIds to
   * match. When users mutate the copied table, and then try to perform some checks joining the
   * two tables, optimizations that depend on `tableId` alone may not be correct. Hence we use a
   * composite id.
   */
  private[delta] def compositeId: (String, Path) = tableId -> dataPath

  /**
   * Run `body` inside `deltaLogLock` lock using `lockInterruptibly` so that the thread can be
   * interrupted when waiting for the lock.
   */
  def lockInterruptibly[T](body: => T): T = {
    deltaLogLock.lockInterruptibly()
    try {
      body
    } finally {
      deltaLogLock.unlock()
    }
  }

  /* ------------------ *
   |  Delta Management  |
   * ------------------ */

  /**
   * Returns a new [[OptimisticTransaction]] that can be used to read the current state of the
   * log and then commit updates. The reads and updates will be checked for logical conflicts
   * with any concurrent writes to the log.
   *
   * Note that all reads in a transaction must go through the returned transaction object, and not
   * directly to the [[DeltaLog]] otherwise they will not be checked for conflicts.
   */
  def startTransaction(): OptimisticTransaction = {
    update()
    new OptimisticTransaction(this)
  }

  /**
   * Execute a piece of code within a new [[OptimisticTransaction]]. Reads/write sets will
   * be recorded for this table, and all other tables will be read
   * at a snapshot that is pinned on the first access.
   *
   * @note This uses thread-local variable to make the active transaction visible. So do not use
   *       multi-threaded code in the provided thunk.
    * 表示一个事物内操作,参数是给定事物对象,返回结果集
   */
  def withNewTransaction[T](thunk: OptimisticTransaction => T): T = {
    try {
      update() //更新当前事物日志的快照
      val txn = new OptimisticTransaction(this) //初始化乐观锁对象
      OptimisticTransaction.setActive(txn) //开启事物
      thunk(txn) //执行写操作
    } finally {
      OptimisticTransaction.clearActive() //关闭事物
    }
  }


  /**
   * Upgrade the table's protocol version, by default to the maximum recognized reader and writer
   * versions in this DBR release.
   */
  def upgradeProtocol(newVersion: Protocol = Protocol()): Unit = {
    val currentVersion = snapshot.protocol
    if (newVersion.minReaderVersion < currentVersion.minReaderVersion ||
        newVersion.minWriterVersion < currentVersion.minWriterVersion) {
      throw new ProtocolDowngradeException(currentVersion, newVersion)
    } else if (newVersion.minReaderVersion == currentVersion.minReaderVersion &&
               newVersion.minWriterVersion == currentVersion.minWriterVersion) {
      logConsole(s"Table $dataPath is already at protocol version $newVersion.")
      return
    }

    val txn = startTransaction()
    try {
      SchemaUtils.checkColumnNameDuplication(txn.metadata.schema, "in the table schema")
    } catch {
      case e: AnalysisException =>
        throw new AnalysisException(
          e.getMessage + "\nPlease remove duplicate columns before you update your table.")
    }
    txn.commit(Seq(newVersion), DeltaOperations.UpgradeProtocol(newVersion))
    logConsole(s"Upgraded table at $dataPath to $newVersion.")
  }

  /**
   * Get all actions starting from "startVersion" (inclusive). If `startVersion` doesn't exist,
   * return an empty Iterator.
    * 返回每一个版本对应的动作集合
   */
  def getChanges(startVersion: Long): Iterator[(Long, Seq[Action])] = {
    val deltas = store.listFrom(deltaFile(logPath, startVersion))
      .filter(f => isDeltaFile(f.getPath)) //找到startVersion开始的json文件
    deltas.map { status =>
      val p = status.getPath
      val version = deltaVersion(p)
      (version, store.read(p).map(Action.fromJson))
    }
  }

  /* --------------------- *
   |  Protocol validation  |
   * --------------------- */

  //根据协议信息，输出协议字符串提示信息
  private def oldProtocolMessage(protocol: Protocol): String =
    s"WARNING: The Delta Lake table at $dataPath has version " +
      s"${protocol.simpleString}, but the latest version is " +
      s"${Protocol().simpleString}. To take advantage of the latest features and bug fixes, " +
      "we recommend that you upgrade the table.\n" +
      "First update all clusters that use this table to the latest version of Databricks " +
      "Runtime, and then run the following command in a notebook:\n" +
      "'%scala com.databricks.delta.Delta.upgradeTable(\"" + s"$dataPath" + "\")'\n\n" +
      "For more information about Delta Lake table versions, see " +
      s"${DeltaErrors.baseDocsPath(spark)}/delta/versioning.html"

  /**
   * If the given `protocol` is older than that of the client.
    * 参数版本过于老
   */
  private def isProtocolOld(protocol: Protocol): Boolean = protocol != null &&
    (Action.readerVersion > protocol.minReaderVersion ||
      Action.writerVersion > protocol.minWriterVersion)

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to read the table that is using the given `protocol`.
   */
  def protocolRead(protocol: Protocol): Unit = {
    if (protocol != null &&
        Action.readerVersion < protocol.minReaderVersion) {
      recordDeltaEvent(
        this,
        "delta.protocol.failure.read",
        data = Map(
          "clientVersion" -> Action.readerVersion,
          "minReaderVersion" -> protocol.minReaderVersion))
      throw new InvalidProtocolVersionException
    }

    if (isProtocolOld(protocol)) {
      recordDeltaEvent(this, "delta.protocol.warning")
      logConsole(oldProtocolMessage(protocol))
    }
  }

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to write to the table that is using the given `protocol`.
   */
  def protocolWrite(protocol: Protocol, logUpgradeMessage: Boolean = true): Unit = {
    if (protocol != null && Action.writerVersion < protocol.minWriterVersion) {
      recordDeltaEvent(
        this,
        "delta.protocol.failure.write",
        data = Map(
          "clientVersion" -> Action.writerVersion,
          "minWriterVersion" -> protocol.minWriterVersion))
      throw new InvalidProtocolVersionException
    }

    if (logUpgradeMessage && isProtocolOld(protocol)) {
      recordDeltaEvent(this, "delta.protocol.warning")
      logConsole(oldProtocolMessage(protocol))
    }
  }

  /* ---------------------------------------- *
   |  Log Directory Management and Retention  |
   * ---------------------------------------- */

  def isValid(): Boolean = {
    val expectedExistingFile = deltaFile(logPath, currentSnapshot.version) //是否有指定的版本json文件
    try {
      store.listFrom(expectedExistingFile)
        .take(1)
        .exists(_.getPath.getName == expectedExistingFile.getName)
    } catch {
      case _: FileNotFoundException =>
        // Parent of expectedExistingFile doesn't exist
        false
    }
  }

  def isSameLogAs(otherLog: DeltaLog): Boolean = this.compositeId == otherLog.compositeId

  /** Creates the log directory if it does not exist. */
  def ensureLogDirectoryExist(): Unit = {
    if (!fs.exists(logPath)) {
      if (!fs.mkdirs(logPath)) {
        throw new IOException(s"Cannot create $logPath")
      }
    }
  }

  /* ------------  *
   |  Integration  |
   * ------------  */

  /**
   * Returns a [[org.apache.spark.sql.DataFrame]] containing the new files within the specified
   * version range.
   */
  def createDataFrame(
      snapshot: Snapshot,
      addFiles: Seq[AddFile],
      isStreaming: Boolean = false,
      actionTypeOpt: Option[String] = None): DataFrame = {
    val actionType = actionTypeOpt.getOrElse(if (isStreaming) "streaming" else "batch")
    val fileIndex = new TahoeBatchFileIndex(spark, actionType, addFiles, this, dataPath, snapshot)

    val relation = HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshot.metadata.partitionSchema,
      dataSchema = snapshot.metadata.schema,
      bucketSpec = None,
      snapshot.fileFormat,
      snapshot.metadata.format.options)(spark)

    Dataset.ofRows(spark, LogicalRelation(relation, isStreaming = isStreaming))
  }

  /**
   * Returns a [[BaseRelation]] that contains all of the data present
   * in the table. This relation will be continually updated
   * as files are added or removed from the table. However, new [[BaseRelation]]
   * must be requested in order to see changes to the schema.
    * 根据分区信息,读取数据源
   */
  def createRelation(
      partitionFilters: Seq[Expression] = Nil,
      timeTravel: Option[DeltaTimeTravelSpec] = None): BaseRelation = {

    //版本号
    val versionToUse = timeTravel.map { tt =>
      //返回版本信息,以及对应的形式(时间戳、版本号)
      val (version, accessType) = DeltaTableUtils.resolveTimeTravelVersion(
        spark.sessionState.conf, this, tt)
      val source = tt.creationSource.getOrElse("unknown") //数据源
      //记录日志
      recordDeltaEvent(this, s"delta.timeTravel.$source", data = Map(
        "tableVersion" -> snapshot.version,
        "queriedVersion" -> version,
        "accessType" -> accessType
      ))
      version
    }

    /** Used to link the files present in the table into the query planner. */
    val fileIndex = TahoeLogFileIndex(spark, this, dataPath, partitionFilters, versionToUse)
    val snapshotToUse = versionToUse.map(getSnapshotAt(_)).getOrElse(snapshot) //版本对象

    new HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshotToUse.metadata.partitionSchema,
      dataSchema = snapshotToUse.metadata.schema,
      bucketSpec = None,
      snapshotToUse.fileFormat,
      snapshotToUse.metadata.format.options)(spark) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
        WriteIntoDelta(
          deltaLog = DeltaLog.this,
          mode = mode,
          new DeltaOptions(Map.empty[String, String], spark.sessionState.conf),
          partitionColumns = Seq.empty,
          configuration = Map.empty,
          data = data).run(spark)
      }
    }
  }
}

object DeltaLog extends DeltaLogging {

  /**
   * We create only a single [[DeltaLog]] for any given path to avoid wasted work
   * in reconstructing the log.
    * 单例模式,每一个path,path为root/log目录，存储一个DeltaLog对象，被缓存在内存中 ,因为 object对象相当于static静态方法
   */
  private val deltaLogCache = {
    val builder = CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES) //缓存过期时间
      .removalListener(new RemovalListener[Path, DeltaLog] { //删除时候触发该事件
        override def onRemoval(removalNotification: RemovalNotification[Path, DeltaLog]) = {
          val log = removalNotification.getValue
          try log.snapshot.uncache() catch {//调用快照的uncache方法
            case _: java.lang.NullPointerException =>
              // Various layers will throw null pointer if the RDD is already gone.
          }
        }
      })
    sys.props.get("delta.log.cacheSize")
      .flatMap(v => Try(v.toLong).toOption)
      .foreach(builder.maximumSize) //设置maximumSize属性值
    builder.build[Path, DeltaLog]()
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: File): DeltaLog = {
    apply(spark, new Path(dataPath.getAbsolutePath, "_delta_log"), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String, clock: Clock): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), clock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: File, clock: Clock): DeltaLog = {
    apply(spark, new Path(dataPath.getAbsolutePath, "_delta_log"), clock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path, clock: Clock): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), clock)
  }
  // TODO: Don't assume the data path here.
  def apply(spark: SparkSession, rawPath: Path, clock: Clock = new SystemClock): DeltaLog = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)
    // The following cases will still create a new ActionLog even if there is a cached
    // ActionLog using a different format path:
    // - Different `scheme`
    // - Different `authority` (e.g., different user tokens in the path)
    // - Different mount point.
    val cached = try {
      //从缓存中获取，如果不存在则创建
      deltaLogCache.get(path, new Callable[DeltaLog] {
        override def call(): DeltaLog = recordDeltaOperation(
            null, "delta.log.create", Map(TAG_TAHOE_PATH -> path.getParent.toString)) {//操作类型是创建表
          AnalysisHelper.allowInvokingTransformsInAnalyzer {//执行方法
            new DeltaLog(path, path.getParent, clock)
          }
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }

    // Invalidate the cache if the reference is no longer valid as a result of the
    // log being deleted.
    if (cached.snapshot.version == -1 || cached.isValid) {
      cached
    } else {
      deltaLogCache.invalidate(path) //让该key从缓存中失效
      apply(spark, path)
    }
  }

  /** Invalidate the cached DeltaLog object for the given `dataPath`.
    * 让路径失效
    **/
  def invalidateCache(spark: SparkSession, dataPath: Path): Unit = {
    try {
      val rawPath = new Path(dataPath, "_delta_log")
      val fs = rawPath.getFileSystem(spark.sessionState.newHadoopConf())
      val path = fs.makeQualified(rawPath)
      deltaLogCache.invalidate(path)
    } catch {
      case NonFatal(e) => logWarning(e.getMessage, e)
    }
  }

  def clearCache(): Unit = {
    deltaLogCache.invalidateAll()
  }

  /**
   * Filters the given [[Dataset]] by the given `partitionFilters`, returning those that match.
   * @param files The active files in the DeltaLog state, which contains the partition value
   *              information
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
   */
  def filterFileList(
      partitionSchema: StructType,//分区的schema
      files: DataFrame,
      partitionFilters: Seq[Expression],//分区表达式
      partitionColumnPrefixes: Seq[String] = Nil): DataFrame = {
    val rewrittenFilters = rewritePartitionFilters(
      partitionSchema,
      files.sparkSession.sessionState.conf.resolver,
      partitionFilters,
      partitionColumnPrefixes)
    //加入分区表达式,如果没有分区内容,则 返回true，表示任何数据都通过
    val columnFilter = new Column(rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true)))
    files.filter(columnFilter)
  }

  /**
   * Rewrite the given `partitionFilters` to be used for filtering partition values.
   * We need to explicitly resolve the partitioning columns here because the partition columns
   * are stored as keys of a Map type instead of attributes in the AddFile schema (below) and thus
   * cannot be resolved automatically.
   *
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
   */
  def rewritePartitionFilters(
      partitionSchema: StructType,
      resolver: Resolver,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil): Seq[Expression] = {
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // If we have a special column name, e.g. `a.a`, then an UnresolvedAttribute returns
        // the column name as '`a.a`' instead of 'a.a', therefore we need to strip the backticks.
        val unquoted = a.name.stripPrefix("`").stripSuffix("`") //去掉`
        val partitionCol = partitionSchema.find { field => resolver(field.name, unquoted) }
        partitionCol match {
          case Some(StructField(name, dataType, _, _)) => //说明a.a是分区列
            Cast(
              UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", name)),
              dataType) //设置哪些列是分区列
          case None =>
            // This should not be able to happen, but the case was present in the original code so
            // we kept it to be safe.
            log.error(s"Partition filter referenced column ${a.name} not in the partition schema")
            UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", a.name))
        }
    })
  }
}
