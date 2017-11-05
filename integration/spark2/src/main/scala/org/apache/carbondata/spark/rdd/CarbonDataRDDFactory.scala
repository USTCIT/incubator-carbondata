/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.concurrent._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.rdd.{DataLoadCoalescedRDD, DataLoadPartitionCoalescer, NewHadoopRDD, RDD}
import org.apache.spark.sql.{CarbonEnv, DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.command.{AlterTableModel, CompactionModel, ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.block.{Distributable, TableBlockInfo}
import org.apache.carbondata.core.dictionary.server.DictionaryServer
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier, ColumnarFormatVersion}
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.scan.partition.PartitionUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.exception.DataLoadingException
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.csvinput.BlockDetails
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.loading.sort.SortScopeOptions
import org.apache.carbondata.processing.merger.{CarbonCompactionUtil, CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.{DataLoadResultImpl, PartitionFactory, _}
import org.apache.carbondata.spark.load._
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CommonUtil, Util}

/**
 * This is the factory class which can create different RDD depends on user needs.
 *
 */
object CarbonDataRDDFactory {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def alterTableForCompaction(sqlContext: SQLContext,
      alterTableModel: AlterTableModel,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String): Unit = {
    var compactionSize: Long = 0
    var compactionType: CompactionType = CompactionType.MINOR_COMPACTION
    if (alterTableModel.compactionType.equalsIgnoreCase("major")) {
      compactionSize = CarbonDataMergerUtil.getCompactionSize(CompactionType.MAJOR_COMPACTION)
      compactionType = CompactionType.MAJOR_COMPACTION
    } else if (alterTableModel.compactionType
      .equalsIgnoreCase(CompactionType.IUD_UPDDEL_DELTA_COMPACTION.toString)) {
      compactionType = CompactionType.IUD_UPDDEL_DELTA_COMPACTION
      if (alterTableModel.segmentUpdateStatusManager.get != None) {
        carbonLoadModel
          .setSegmentUpdateStatusManager(alterTableModel.segmentUpdateStatusManager.get)

        carbonLoadModel
          .setLoadMetadataDetails(alterTableModel.segmentUpdateStatusManager.get
            .getLoadMetadataDetails.toList.asJava)
      }
    } else if (alterTableModel.compactionType.
      equalsIgnoreCase(CompactionType.SEGMENT_INDEX_COMPACTION.toString)) {
      compactionType = CompactionType.SEGMENT_INDEX_COMPACTION
    } else {
      compactionType = CompactionType.MINOR_COMPACTION
    }

    LOGGER.audit(s"Compaction request received for table " +
        s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable

    if (null == carbonLoadModel.getLoadMetadataDetails) {
      CommonUtil.readLoadMetadataDetails(carbonLoadModel)
    }
    if (compactionType == CompactionType.SEGMENT_INDEX_COMPACTION) {
      // Just launch job to merge index and return
      CommonUtil.mergeIndexFiles(sqlContext.sparkContext,
        carbonLoadModel.getLoadMetadataDetails.asScala.map(_.getLoadName),
        carbonLoadModel.getStorePath,
        carbonTable)
      return
    }
    // reading the start time of data load.
    val loadStartTime : Long =
    if (alterTableModel.factTimeStamp.isEmpty) {
      CarbonUpdateUtil.readCurrentTime
    } else {
      alterTableModel.factTimeStamp.get
    }
    carbonLoadModel.setFactTimeStamp(loadStartTime)

    val isCompactionTriggerByDDl = true
    val compactionModel = CompactionModel(compactionSize,
      compactionType,
      carbonTable,
      isCompactionTriggerByDDl
    )

    val isConcurrentCompactionAllowed = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION,
          CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION
        )
        .equalsIgnoreCase("true")

    // if system level compaction is enabled then only one compaction can run in the system
    // if any other request comes at this time then it will create a compaction request file.
    // so that this will be taken up by the compaction process which is executing.
    if (!isConcurrentCompactionAllowed) {
      LOGGER.info("System level compaction lock is enabled.")
      handleCompactionForSystemLocking(sqlContext,
        carbonLoadModel,
        storeLocation,
        compactionType,
        carbonTable,
        compactionModel
      )
    } else {
      // normal flow of compaction
      val lock = CarbonLockFactory
          .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
            LockUsage.COMPACTION_LOCK
          )

      if (lock.lockWithRetries()) {
        LOGGER.info("Acquired the compaction lock for table" +
            s" ${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        try {
          startCompactionThreads(sqlContext,
            carbonLoadModel,
            storeLocation,
            compactionModel,
            lock
          )
        } catch {
          case e: Exception =>
            LOGGER.error(s"Exception in start compaction thread. ${ e.getMessage }")
            lock.unlock()
            throw e
        }
      } else {
        LOGGER.audit("Not able to acquire the compaction lock for table " +
            s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.error(s"Not able to acquire the compaction lock for table" +
            s" ${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        sys.error("Table is already locked for compaction. Please try after some time.")
      }
    }
  }

  def alterTableSplitPartition(sqlContext: SQLContext,
      partitionId: String,
      carbonLoadModel: CarbonLoadModel,
      oldPartitionIdList: List[Int]): Unit = {
    LOGGER.audit(s"Add partition request received for table " +
         s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
    try {
      startSplitThreads(sqlContext,
        carbonLoadModel,
        partitionId,
        oldPartitionIdList)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception in start splitting partition thread. ${ e.getMessage }")
        throw e
    }
  }

  def alterTableDropPartition(sqlContext: SQLContext,
      partitionId: String,
      carbonLoadModel: CarbonLoadModel,
      dropWithData: Boolean,
      oldPartitionIds: List[Int]): Unit = {
    LOGGER.audit(s"Drop partition request received for table " +
                 s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
    try {
      startDropThreads(sqlContext,
        carbonLoadModel,
        partitionId,
        dropWithData,
        oldPartitionIds)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception in start dropping partition thread. ${ e.getMessage }")
        throw e
    }
  }

  def handleCompactionForSystemLocking(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String,
      compactionType: CompactionType,
      carbonTable: CarbonTable,
      compactionModel: CompactionModel): Unit = {
    val lock = CarbonLockFactory
        .getCarbonLockObj(CarbonCommonConstants.SYSTEM_LEVEL_COMPACTION_LOCK_FOLDER,
          LockUsage.SYSTEMLEVEL_COMPACTION_LOCK
        )
    if (lock.lockWithRetries()) {
      LOGGER.info(s"Acquired the compaction lock for table ${ carbonLoadModel.getDatabaseName }" +
          s".${ carbonLoadModel.getTableName }")
      try {
        startCompactionThreads(sqlContext,
          carbonLoadModel,
          storeLocation,
          compactionModel,
          lock
        )
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in start compaction thread. ${ e.getMessage }")
          lock.unlock()
          // if the compaction is a blocking call then only need to throw the exception.
          if (compactionModel.isDDLTrigger) {
            throw e
          }
      }
    } else {
      LOGGER.audit("Not able to acquire the system level compaction lock for table " +
          s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      LOGGER.error("Not able to acquire the compaction lock for table " +
          s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      CarbonCompactionUtil
          .createCompactionRequiredFile(carbonTable.getMetaDataFilepath, compactionType)
      // do sys error only in case of DDL trigger.
      if (compactionModel.isDDLTrigger) {
        sys.error("Compaction is in progress, compaction request for table " +
            s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }" +
            " is in queue.")
      } else {
        LOGGER.error("Compaction is in progress, compaction request for table " +
            s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }" +
            " is in queue.")
      }
    }
  }

  def startCompactionThreads(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String,
      compactionModel: CompactionModel,
      compactionLock: ICarbonLock): Unit = {
    val executor: ExecutorService = Executors.newFixedThreadPool(1)
    // update the updated table status.
    if (compactionModel.compactionType != CompactionType.IUD_UPDDEL_DELTA_COMPACTION) {
      // update the updated table status. For the case of Update Delta Compaction the Metadata
      // is filled in LoadModel, no need to refresh.
      CommonUtil.readLoadMetadataDetails(carbonLoadModel)
    }

    val compactionThread = new Thread {
      override def run(): Unit = {

        try {
          // compaction status of the table which is triggered by the user.
          var triggeredCompactionStatus = false
          var exception: Exception = null
          try {
            DataManagementFunc.executeCompaction(carbonLoadModel: CarbonLoadModel,
              compactionModel: CompactionModel,
              executor, sqlContext, storeLocation
            )
            triggeredCompactionStatus = true
          } catch {
            case e: Exception =>
              LOGGER.error(s"Exception in compaction thread ${ e.getMessage }")
              exception = e
          }
          // continue in case of exception also, check for all the tables.
          val isConcurrentCompactionAllowed = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION,
                CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION
              ).equalsIgnoreCase("true")

          if (!isConcurrentCompactionAllowed) {
            LOGGER.info("System level compaction lock is enabled.")
            val skipCompactionTables = ListBuffer[CarbonTableIdentifier]()
            var tableForCompaction = CarbonCompactionUtil
              .getNextTableToCompact(CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetastore
                .listAllTables(sqlContext.sparkSession).toArray,
                skipCompactionTables.toList.asJava)
            while (null != tableForCompaction) {
              LOGGER.info("Compaction request has been identified for table " +
                  s"${ tableForCompaction.getDatabaseName }." +
                  s"${ tableForCompaction.getFactTableName}")
              val table: CarbonTable = tableForCompaction
              val metadataPath = table.getMetaDataFilepath
              val compactionType = CarbonCompactionUtil.determineCompactionType(metadataPath)

              val newCarbonLoadModel = new CarbonLoadModel()
              DataManagementFunc.prepareCarbonLoadModel(table, newCarbonLoadModel)

              val compactionSize = CarbonDataMergerUtil
                  .getCompactionSize(CompactionType.MAJOR_COMPACTION)

              val newcompactionModel = CompactionModel(compactionSize,
                compactionType,
                table,
                compactionModel.isDDLTrigger
              )
              // proceed for compaction
              try {
                DataManagementFunc.executeCompaction(newCarbonLoadModel,
                  newcompactionModel,
                  executor, sqlContext, storeLocation
                )
              } catch {
                case e: Exception =>
                  LOGGER.error("Exception in compaction thread for table " +
                      s"${ tableForCompaction.getDatabaseName }." +
                      s"${ tableForCompaction.getFactTableName }")
                // not handling the exception. only logging as this is not the table triggered
                // by user.
              } finally {
                // delete the compaction required file in case of failure or success also.
                if (!CarbonCompactionUtil
                    .deleteCompactionRequiredFile(metadataPath, compactionType)) {
                  // if the compaction request file is not been able to delete then
                  // add those tables details to the skip list so that it wont be considered next.
                  skipCompactionTables.+=:(tableForCompaction.getCarbonTableIdentifier)
                  LOGGER.error("Compaction request file can not be deleted for table " +
                      s"${ tableForCompaction.getDatabaseName }." +
                      s"${ tableForCompaction.getFactTableName }")
                }
              }
              // ********* check again for all the tables.
              tableForCompaction = CarbonCompactionUtil
                .getNextTableToCompact(CarbonEnv.getInstance(sqlContext.sparkSession)
                  .carbonMetastore.listAllTables(sqlContext.sparkSession).toArray,
                  skipCompactionTables.asJava
                )
            }
          }
          // giving the user his error for telling in the beeline if his triggered table
          // compaction is failed.
          if (!triggeredCompactionStatus) {
            throw new Exception("Exception in compaction " + exception.getMessage)
          }
        } finally {
          executor.shutdownNow()
          DataManagementFunc.deletePartialLoadsInCompaction(carbonLoadModel)
          compactionLock.unlock()
        }
      }
    }
    // calling the run method of a thread to make the call as blocking call.
    // in the future we may make this as concurrent.
    compactionThread.run()
  }

  case class SplitThread(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      executor: ExecutorService,
      segmentId: String,
      partitionId: String,
      oldPartitionIdList: List[Int]) extends Thread {
      override def run(): Unit = {
        var triggeredSplitPartitionStatus = false
        var exception: Exception = null
        try {
          DataManagementFunc.executePartitionSplit(sqlContext,
            carbonLoadModel, executor, segmentId, partitionId, oldPartitionIdList)
          triggeredSplitPartitionStatus = true
        } catch {
          case e: Exception =>
            LOGGER.error(s"Exception in partition split thread: ${ e.getMessage } }")
          exception = e
        }
        if (triggeredSplitPartitionStatus == false) {
          throw new Exception("Exception in split partition " + exception.getMessage)
        }
      }
  }

  case class dropPartitionThread(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      executor: ExecutorService,
      segmentId: String,
      partitionId: String,
      dropWithData: Boolean,
      oldPartitionIds: List[Int]) extends Thread {
    override def run(): Unit = {
      try {
        DataManagementFunc.executeDroppingPartition(sqlContext, carbonLoadModel, executor,
          segmentId, partitionId, dropWithData, oldPartitionIds)
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in dropping partition thread: ${ e.getMessage } }")
      }
    }
  }

  def startSplitThreads(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      partitionId: String,
      oldPartitionIdList: List[Int]): Unit = {
    val numberOfCores = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.NUM_CORES_ALT_PARTITION,
        CarbonCommonConstants.DEFAULT_NUMBER_CORES)
    val executor : ExecutorService = Executors.newFixedThreadPool(numberOfCores.toInt)
    try {
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      val segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier)
      val validSegments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala
      val threadArray: Array[SplitThread] = new Array[SplitThread](validSegments.size)
      var i = 0
      validSegments.foreach { segmentId =>
        threadArray(i) = SplitThread(sqlContext, carbonLoadModel, executor,
          segmentId, partitionId, oldPartitionIdList)
        threadArray(i).start()
        i += 1
      }
      threadArray.foreach {
        thread => thread.join()
      }
      val identifier = AbsoluteTableIdentifier.from(carbonLoadModel.getStorePath,
        carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)
      val refresher = DataMapStoreManager.getInstance().getTableSegmentRefresher(identifier)
      refresher.refreshSegments(validSegments.asJava)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception when split partition: ${ e.getMessage }")
      throw e
    } finally {
      executor.shutdown()
      try {
        CarbonLoaderUtil.deletePartialLoadDataIfExist(carbonLoadModel, false)
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in add/split partition thread while deleting partial load file" +
                       s" ${ e.getMessage }")
      }
    }
  }

  def startDropThreads(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      partitionId: String,
      dropWithData: Boolean,
      oldPartitionIds: List[Int]): Unit = {
    val numberOfCores = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.NUM_CORES_ALT_PARTITION,
      CarbonCommonConstants.DEFAULT_NUMBER_CORES)
    val executor : ExecutorService = Executors.newFixedThreadPool(numberOfCores.toInt)
    try {
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      val segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier)
      val validSegments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala
      val threadArray: Array[Thread] = new Array[Thread](validSegments.size)
      var i = 0
      for (segmentId: String <- validSegments) {
        threadArray(i) = dropPartitionThread(sqlContext, carbonLoadModel, executor,
            segmentId, partitionId, dropWithData, oldPartitionIds)
        threadArray(i).start()
        i += 1
      }
      for (thread <- threadArray) {
        thread.join()
      }
      val identifier = AbsoluteTableIdentifier.from(carbonLoadModel.getStorePath,
        carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)
      val refresher = DataMapStoreManager.getInstance().getTableSegmentRefresher(identifier)
      refresher.refreshSegments(validSegments.asJava)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception when dropping partition: ${ e.getMessage }")
    } finally {
      executor.shutdown()
      try {
        CarbonLoaderUtil.deletePartialLoadDataIfExist(carbonLoadModel, false)
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in dropping partition thread while deleting partial load file" +
                       s" ${ e.getMessage }")
      }
    }
  }

  def loadCarbonData(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storePath: String,
      columnar: Boolean,
      partitionStatus: String = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS,
      result: Option[DictionaryServer],
      overwriteTable: Boolean,
      dataFrame: Option[DataFrame] = None,
      updateModel: Option[UpdateTableModel] = None): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    // for handling of the segment Merging.
    def handleSegmentMerging(): Unit = {
      LOGGER.info(s"compaction need status is" +
          s" ${ CarbonDataMergerUtil.checkIfAutoLoadMergingRequired() }")
      if (CarbonDataMergerUtil.checkIfAutoLoadMergingRequired()) {
        LOGGER.audit(s"Compaction request received for table " +
            s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        val compactionSize = 0
        val isCompactionTriggerByDDl = false
        val compactionModel = CompactionModel(compactionSize,
          CompactionType.MINOR_COMPACTION,
          carbonTable,
          isCompactionTriggerByDDl
        )
        var storeLocation = ""
        val configuredStore = Util.getConfiguredLocalDirs(SparkEnv.get.conf)
        if (null != configuredStore && configuredStore.nonEmpty) {
          storeLocation = configuredStore(Random.nextInt(configuredStore.length))
        }
        if (storeLocation == null) {
          storeLocation = System.getProperty("java.io.tmpdir")
        }
        storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()

        val isConcurrentCompactionAllowed = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION,
              CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION
            )
            .equalsIgnoreCase("true")

        if (!isConcurrentCompactionAllowed) {

          handleCompactionForSystemLocking(sqlContext,
            carbonLoadModel,
            storeLocation,
            CompactionType.MINOR_COMPACTION,
            carbonTable,
            compactionModel
          )
        } else {
          val lock = CarbonLockFactory
              .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
                LockUsage.COMPACTION_LOCK
              )

          if (lock.lockWithRetries()) {
            LOGGER.info("Acquired the compaction lock.")
            try {
              startCompactionThreads(sqlContext,
                carbonLoadModel,
                storeLocation,
                compactionModel,
                lock
              )
            } catch {
              case e: Exception =>
                LOGGER.error(s"Exception in start compaction thread. ${ e.getMessage }")
                lock.unlock()
                throw e
            }
          } else {
            LOGGER.audit("Not able to acquire the compaction lock for table " +
                s"${ carbonLoadModel.getDatabaseName }.${
                  carbonLoadModel
                      .getTableName
                }")
            LOGGER.error("Not able to acquire the compaction lock for table " +
                s"${ carbonLoadModel.getDatabaseName }.${
                  carbonLoadModel
                      .getTableName
                }")
          }
        }
      }
    }

    def updateStatus(status: Array[(String, (LoadMetadataDetails, ExecutionErrors))],
        loadStatus: String) = {
      val metadataDetails = if (status != null && status(0) != null) {
        status(0)._2._1
      } else {
        new LoadMetadataDetails
      }
      CarbonLoaderUtil
        .populateNewLoadMetaEntry(metadataDetails,
          loadStatus,
          carbonLoadModel.getFactTimeStamp,
          true)
      val success = CarbonLoaderUtil.recordLoadMetadata(metadataDetails,
        carbonLoadModel, false, overwriteTable)
      if (!success) {
        val errorMessage = "Dataload failed due to failure in table status updation."
        LOGGER.audit("Data load is failed for " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.error("Dataload failed due to failure in table status updation.")
        throw new Exception(errorMessage)
      } else if (!carbonLoadModel.isRetentionRequest) {
        // TODO : Handle it
        LOGGER.info("********Database updated**********")
      }
    }

    try {
      LOGGER.audit(s"Data load request has been received for table" +
          s" ${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      // Check if any load need to be deleted before loading new data
      DataManagementFunc.deleteLoadsAndUpdateMetadata(carbonLoadModel.getDatabaseName,
        carbonLoadModel.getTableName, storePath, false, carbonTable)
      var blocksGroupBy: Array[(String, Array[BlockDetails])] = null
      var status: Array[(String, (LoadMetadataDetails, ExecutionErrors))] = null
      var res: Array[List[(String, (LoadMetadataDetails, ExecutionErrors))]] = null

      def loadDataFile(): Unit = {
        /*
         * when data load handle by node partition
         * 1)clone the hadoop configuration,and set the file path to the configuration
         * 2)use org.apache.hadoop.mapreduce.lib.input.TextInputFormat to get splits,size info
         * 3)use CarbonLoaderUtil.nodeBlockMapping to get mapping info of node and block,
         *   for locally writing carbondata files(one file one block) in nodes
         * 4)use NewCarbonDataLoadRDD to load data and write to carbondata files
         */
        val hadoopConfiguration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
        // FileUtils will skip file which is no csv, and return all file path which split by ','
        val filePaths = carbonLoadModel.getFactFilePath
        hadoopConfiguration.set(FileInputFormat.INPUT_DIR, filePaths)
        hadoopConfiguration.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
        hadoopConfiguration.set("io.compression.codecs",
          """org.apache.hadoop.io.compress.GzipCodec,
             org.apache.hadoop.io.compress.DefaultCodec,
             org.apache.hadoop.io.compress.BZip2Codec""".stripMargin)

        CommonUtil.configSplitMaxSize(sqlContext.sparkContext, filePaths, hadoopConfiguration)

        val inputFormat = new org.apache.hadoop.mapreduce.lib.input.TextInputFormat
        val jobContext = new Job(hadoopConfiguration)
        val rawSplits = inputFormat.getSplits(jobContext).toArray
        val blockList = rawSplits.map { inputSplit =>
          val fileSplit = inputSplit.asInstanceOf[FileSplit]
          new TableBlockInfo(fileSplit.getPath.toString,
            fileSplit.getStart, "1",
            fileSplit.getLocations, fileSplit.getLength, ColumnarFormatVersion.V1, null
          ).asInstanceOf[Distributable]
        }
        // group blocks to nodes, tasks
        val startTime = System.currentTimeMillis
        val activeNodes = DistributionUtil
            .ensureExecutorsAndGetNodeList(blockList, sqlContext.sparkContext)
        val nodeBlockMapping =
          CarbonLoaderUtil
              .nodeBlockMapping(blockList.toSeq.asJava, -1, activeNodes.toList.asJava).asScala
              .toSeq
        val timeElapsed: Long = System.currentTimeMillis - startTime
        LOGGER.info("Total Time taken in block allocation: " + timeElapsed)
        LOGGER.info(s"Total no of blocks: ${ blockList.length }, " +
            s"No.of Nodes: ${nodeBlockMapping.size}")
        var str = ""
        nodeBlockMapping.foreach(entry => {
          val tableBlock = entry._2
          str = str + "#Node: " + entry._1 + " no.of.blocks: " + tableBlock.size()
          tableBlock.asScala.foreach(tableBlockInfo =>
            if (!tableBlockInfo.getLocations.exists(hostentry =>
              hostentry.equalsIgnoreCase(entry._1)
            )) {
              str = str + " , mismatch locations: " + tableBlockInfo.getLocations
                  .foldLeft("")((a, b) => a + "," + b)
            }
          )
          str = str + "\n"
        }
        )
        LOGGER.info(str)
        blocksGroupBy = nodeBlockMapping.map { entry =>
          val blockDetailsList =
            entry._2.asScala.map { distributable =>
              val tableBlock = distributable.asInstanceOf[TableBlockInfo]
              new BlockDetails(new Path(tableBlock.getFilePath),
                tableBlock.getBlockOffset, tableBlock.getBlockLength, tableBlock.getLocations
              )
            }.toArray
          (entry._1, blockDetailsList)
        }.toArray

        status = new NewCarbonDataLoadRDD(
          sqlContext.sparkContext,
          new DataLoadResultImpl(),
          carbonLoadModel,
          blocksGroupBy
        ).collect()
      }

      def loadDataFrame(): Unit = {
        try {
          val rdd = dataFrame.get.rdd

          val nodeNumOfData = rdd.partitions.flatMap[String, Array[String]]{ p =>
            DataLoadPartitionCoalescer.getPreferredLocs(rdd, p).map(_.host)
          }.distinct.size
          val nodes = DistributionUtil.ensureExecutorsByNumberAndGetNodeList(nodeNumOfData,
            sqlContext.sparkContext)
          val newRdd = new DataLoadCoalescedRDD[Row](rdd, nodes.toArray.distinct)

          status = new NewDataFrameLoaderRDD(sqlContext.sparkContext,
            new DataLoadResultImpl(),
            carbonLoadModel,
            newRdd).collect()

        } catch {
          case ex: Exception =>
            LOGGER.error(ex, "load data frame failed")
            throw ex
        }
      }

      def loadDataFrameForUpdate(): Unit = {
        val segmentUpdateParallelism = CarbonProperties.getInstance().getParallelismForSegmentUpdate

        def triggerDataLoadForSegment(key: String, taskNo: Int,
            iter: Iterator[Row]): Iterator[(String, (LoadMetadataDetails, ExecutionErrors))] = {
          val rddResult = new updateResultImpl()
          val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
          val resultIter = new Iterator[(String, (LoadMetadataDetails, ExecutionErrors))] {
            var partitionID = "0"
            val loadMetadataDetails = new LoadMetadataDetails
            val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
            var uniqueLoadStatusId = ""
            try {
              val segId = key
              val index = taskNo
              uniqueLoadStatusId = carbonLoadModel.getTableName +
                                   CarbonCommonConstants.UNDERSCORE +
                                   (index + "_0")

              // convert timestamp
              val timeStampInLong = updateModel.get.updatedTimeStamp + ""
              loadMetadataDetails.setPartitionCount(partitionID)
              loadMetadataDetails.setLoadName(segId)
              loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)
              carbonLoadModel.setPartitionId(partitionID)
              carbonLoadModel.setSegmentId(segId)
              carbonLoadModel.setTaskNo(String.valueOf(index))
              carbonLoadModel.setFactTimeStamp(updateModel.get.updatedTimeStamp)

              // During Block Spill case Increment of File Count and proper adjustment of Block
              // naming is only done when AbstractFactDataWriter.java : initializeWriter get
              // CarbondataFileName as null. For handling Block Spill not setting the
              // CarbondataFileName in case of Update.
              // carbonLoadModel.setCarbondataFileName(newBlockName)

              // storeLocation = CarbonDataLoadRDD.initialize(carbonLoadModel, index)
              loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
              UpdateDataLoad.DataLoadForUpdate(segId,
                index,
                iter,
                carbonLoadModel,
                loadMetadataDetails)
            } catch {
              case e: NoRetryException =>
                loadMetadataDetails
                  .setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
                executionErrors.failureCauses = FailureCauses.BAD_RECORDS
                executionErrors.errorMsg = e.getMessage
                LOGGER.info("Bad Record Found")
              case e: Exception =>
                LOGGER.info("DataLoad failure")
                LOGGER.error(e)
                throw e
            }

            var finished = false

            override def hasNext: Boolean = !finished

            override def next(): (String, (LoadMetadataDetails, ExecutionErrors)) = {
              finished = true
              rddResult
                .getKey(uniqueLoadStatusId,
                  (loadMetadataDetails, executionErrors))
            }
          }
          resultIter
        }

        val updateRdd = dataFrame.get.rdd

        // return directly if no rows to update
        val noRowsToUpdate = updateRdd.isEmpty()
        if (noRowsToUpdate) {
          res = Array[List[(String, (LoadMetadataDetails, ExecutionErrors))]]()
          return
        }

        // splitting as (key, value) i.e., (segment, updatedRows)
        val keyRDD = updateRdd.map(row =>
            (row.get(row.size - 1).toString, Row(row.toSeq.slice(0, row.size - 1): _*)))

        val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(
          carbonTable.getMetaDataFilepath)
        val segmentIds = loadMetadataDetails.map(_.getLoadName)
        val segmentIdIndex = segmentIds.zipWithIndex.toMap
        val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonLoadModel.getStorePath,
          carbonTable.getCarbonTableIdentifier)
        val segmentId2maxTaskNo = segmentIds.map { segId =>
          (segId, CarbonUpdateUtil.getLatestTaskIdForSegment(segId, carbonTablePath))
        }.toMap

        class SegmentPartitioner(segIdIndex: Map[String, Int], parallelism: Int)
          extends org.apache.spark.Partitioner {
          override def numPartitions: Int = segmentIdIndex.size * parallelism

          override def getPartition(key: Any): Int = {
            val segId = key.asInstanceOf[String]
            // partitionId
            segmentIdIndex(segId) * parallelism + Random.nextInt(parallelism)
          }
        }

        val partitionByRdd = keyRDD.partitionBy(new SegmentPartitioner(segmentIdIndex,
          segmentUpdateParallelism))

        // because partitionId=segmentIdIndex*parallelism+RandomPart and RandomPart<parallelism,
        // so segmentIdIndex=partitionId/parallelism, this has been verified.
        res = partitionByRdd.map(_._2).mapPartitions { partition =>
          val partitionId = TaskContext.getPartitionId()
          val segIdIndex = partitionId / segmentUpdateParallelism
          val randomPart = partitionId - segIdIndex * segmentUpdateParallelism
          val segId = segmentIds(segIdIndex)
          val newTaskNo = segmentId2maxTaskNo(segId) + randomPart + 1

          List(triggerDataLoadForSegment(segId, newTaskNo, partition).toList).toIterator
        }.collect()
      }

      def loadDataForPartitionTable(): Unit = {
        try {
          val rdd = repartitionInputData(sqlContext, dataFrame, carbonLoadModel)
          status = new PartitionTableDataLoaderRDD(sqlContext.sparkContext,
            new DataLoadResultImpl(),
            carbonLoadModel,
            rdd).collect()
        } catch {
          case ex: Exception =>
            LOGGER.error(ex, "load data failed for partition table")
            throw ex
        }
      }
      // create new segment folder  in carbon store
      if (updateModel.isEmpty) {
        CarbonLoaderUtil.checkAndCreateCarbonDataLocation(storePath,
          carbonLoadModel.getSegmentId, carbonTable)
      }
      var loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      var errorMessage: String = "DataLoad failure"
      var executorMessage: String = ""
      val isSortTable = carbonTable.getNumberOfSortColumns > 0
      val sortScope = CarbonDataProcessorUtil.getSortScope(carbonLoadModel.getSortScope)
      try {
        if (updateModel.isDefined) {
          loadDataFrameForUpdate()
        } else if (carbonTable.getPartitionInfo(carbonTable.getFactTableName) != null) {
          loadDataForPartitionTable()
        } else if (isSortTable && sortScope.equals(SortScopeOptions.SortScope.GLOBAL_SORT)) {
          LOGGER.audit("Using global sort for loading.")
          status = DataLoadProcessBuilderOnSpark.loadDataUsingGlobalSort(sqlContext.sparkContext,
            dataFrame, carbonLoadModel)
        } else if (dataFrame.isDefined) {
          loadDataFrame()
        } else {
          loadDataFile()
        }
        if (updateModel.isDefined) {

          res.foreach(resultOfSeg => resultOfSeg.foreach(
            resultOfBlock => {
              if (resultOfBlock._2._1.getLoadStatus
                .equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)) {
                loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
                if (resultOfBlock._2._2.failureCauses == FailureCauses.NONE) {
                  updateModel.get.executorErrors.failureCauses = FailureCauses.EXECUTOR_FAILURE
                  updateModel.get.executorErrors.errorMsg = "Failure in the Executor."
                }
                else {
                  updateModel.get.executorErrors = resultOfBlock._2._2
                }
              } else if (resultOfBlock._2._1.getLoadStatus
                .equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)) {
                loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
                updateModel.get.executorErrors.failureCauses = resultOfBlock._2._2.failureCauses
                updateModel.get.executorErrors.errorMsg = resultOfBlock._2._2.errorMsg
              }
            }
          ))

        } else {
          CommonUtil.mergeIndexFiles(sqlContext.sparkContext,
            Seq(carbonLoadModel.getSegmentId), storePath, carbonTable)
          val newStatusMap = scala.collection.mutable.Map.empty[String, String]
          if (status.nonEmpty) {
            status.foreach { eachLoadStatus =>
              val state = newStatusMap.get(eachLoadStatus._1)
              state match {
                case Some(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) =>
                  newStatusMap.put(eachLoadStatus._1, eachLoadStatus._2._1.getLoadStatus)
                case Some(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
                  if eachLoadStatus._2._1.getLoadStatus ==
                     CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS =>
                  newStatusMap.put(eachLoadStatus._1, eachLoadStatus._2._1.getLoadStatus)
                case _ =>
                  newStatusMap.put(eachLoadStatus._1, eachLoadStatus._2._1.getLoadStatus)
              }
            }

          newStatusMap.foreach {
            case (key, value) =>
              if (value == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
                loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
              } else if (value == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS &&
                  !loadStatus.equals(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)) {
                loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
              }
          }
        } else {
          loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
        }

        if (loadStatus != CarbonCommonConstants.STORE_LOADSTATUS_FAILURE &&
            partitionStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
          loadStatus = partitionStatus
        }
      }
      } catch {
        case ex: Throwable =>
          loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          ex match {
            case sparkException: SparkException =>
              if (sparkException.getCause.isInstanceOf[DataLoadingException] ||
                  sparkException.getCause.isInstanceOf[CarbonDataLoadingException]) {
                executorMessage = sparkException.getCause.getMessage
                errorMessage = errorMessage + ": " + executorMessage
              }
            case _ =>
              if (ex.getCause != null) {
                executorMessage = ex.getCause.getMessage
                errorMessage = errorMessage + ": " + executorMessage
              }
          }
          LOGGER.info(errorMessage)
          LOGGER.error(ex)
      }
      // handle the status file updation for the update cmd.
      if (updateModel.isDefined) {

        if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
          // updateModel.get.executorErrors.errorMsg = errorMessage
          if (updateModel.get.executorErrors.failureCauses == FailureCauses.NONE) {
            updateModel.get.executorErrors.failureCauses = FailureCauses.EXECUTOR_FAILURE
            if (null != executorMessage && !executorMessage.isEmpty) {
              updateModel.get.executorErrors.errorMsg = executorMessage
            } else {
              updateModel.get.executorErrors.errorMsg = "Update failed as the data load has failed."
            }
          }
          return
        } else if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS &&
                   updateModel.get.executorErrors.failureCauses == FailureCauses.BAD_RECORDS &&
                   carbonLoadModel.getBadRecordsAction.split(",")(1) == LoggerAction.FAIL.name) {
          return
        } else {
          // in success case handle updation of the table status file.
          // success case.
          val segmentDetails = new util.HashSet[String]()

          var resultSize = 0

          res.foreach(resultOfSeg => {
            resultSize = resultSize + resultOfSeg.size
            resultOfSeg.foreach(
            resultOfBlock => {
              segmentDetails.add(resultOfBlock._2._1.getLoadName)
            }
          )}
          )

          // this means that the update doesnt have any records to update so no need to do table
          // status file updation.
          if (resultSize == 0) {
            LOGGER.audit("Data update is successful with 0 rows updation for " +
                         s"${carbonLoadModel.getDatabaseName}.${carbonLoadModel.getTableName}")
            return
          }

          if (
            CarbonUpdateUtil
              .updateTableMetadataStatus(segmentDetails,
                carbonTable,
                updateModel.get.updatedTimeStamp + "",
                true,
                new util.ArrayList[String](0))) {
            LOGGER.audit("Data update is successful for " +
                         s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
          }
          else {
            val errorMessage = "Data update failed due to failure in table status updation."
            LOGGER.audit("Data update is failed for " +
                         s"${carbonLoadModel.getDatabaseName}.${carbonLoadModel.getTableName}")
            LOGGER.error("Data update failed due to failure in table status updation.")
            updateModel.get.executorErrors.errorMsg = errorMessage
            updateModel.get.executorErrors.failureCauses = FailureCauses
              .STATUS_FILE_UPDATION_FAILURE
            return
          }

        }

        return
      }
      if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
        // update the load entry in table status file for changing the status to failure
        CommonUtil.updateTableStatusForFailure(carbonLoadModel)
        LOGGER.info("********starting clean up**********")
        CarbonLoaderUtil.deleteSegment(carbonLoadModel, carbonLoadModel.getSegmentId.toInt)
        LOGGER.info("********clean up done**********")
        LOGGER.audit(s"Data load is failed for " +
            s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.warn("Cannot write load metadata file as data load failed")
        throw new Exception(errorMessage)
      } else {
        // check if data load fails due to bad record and throw data load failure due to
        // bad record exception
        if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS &&
            status(0)._2._2.failureCauses == FailureCauses.BAD_RECORDS &&
            carbonLoadModel.getBadRecordsAction.split(",")(1) == LoggerAction.FAIL.name) {
          // update the load entry in table status file for changing the status to failure
          CommonUtil.updateTableStatusForFailure(carbonLoadModel)
          LOGGER.info("********starting clean up**********")
          CarbonLoaderUtil.deleteSegment(carbonLoadModel, carbonLoadModel.getSegmentId.toInt)
          LOGGER.info("********clean up done**********")
          LOGGER.audit(s"Data load is failed for " +
                       s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
          throw new Exception(status(0)._2._2.errorMsg)
        }
        // if segment is empty then fail the data load
        if (!CarbonLoaderUtil.isValidSegment(carbonLoadModel, carbonLoadModel.getSegmentId.toInt)) {
          // update the load entry in table status file for changing the status to failure
          CommonUtil.updateTableStatusForFailure(carbonLoadModel)
          LOGGER.info("********starting clean up**********")
          CarbonLoaderUtil.deleteSegment(carbonLoadModel, carbonLoadModel.getSegmentId.toInt)
          LOGGER.info("********clean up done**********")
          LOGGER.audit(s"Data load is failed for " +
                       s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }" +
                       " as there is no data to load")
          LOGGER.warn("Cannot write load metadata file as data load failed")
          throw new Exception("No Data to load")
        }
        writeDictionary(carbonLoadModel, result, false)
        updateStatus(status, loadStatus)

        if (CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS.equals(loadStatus)) {
          LOGGER.audit("Data load is partially successful for " +
                       s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        } else {
          LOGGER.audit("Data load is successful for " +
                       s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        }
        try {
          // compaction handling
          handleSegmentMerging()
        } catch {
          case e: Exception =>
            throw new Exception(
              "Dataload is success. Auto-Compaction has failed. Please check logs.")
        }
      }
    }

  }


  /**
   * repartition the input data for partition table.
   *
   * @param sqlContext
   * @param dataFrame
   * @param carbonLoadModel
   * @return
   */
  private def repartitionInputData(sqlContext: SQLContext,
      dataFrame: Option[DataFrame],
      carbonLoadModel: CarbonLoadModel): RDD[Row] = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName)
    val partitionColumn = partitionInfo.getColumnSchemaList.get(0).getColumnName
    val partitionColumnDataType = partitionInfo.getColumnSchemaList.get(0).getDataType
    val columns = carbonLoadModel.getCsvHeaderColumns
    var partitionColumnIndex = -1
    breakable {
      for (i <- 0 until columns.length) {
        if (partitionColumn.equalsIgnoreCase(columns(i))) {
          partitionColumnIndex = i
          break
        }
      }
    }
    if (partitionColumnIndex == -1) {
      throw new DataLoadingException("Partition column not found.")
    }

    val dateFormatMap = CarbonDataProcessorUtil.getDateFormatMap(carbonLoadModel.getDateFormat())
    val specificFormat = Option(dateFormatMap.get(partitionColumn.toLowerCase))
    val timeStampFormat = if (specificFormat.isDefined) {
      new SimpleDateFormat(specificFormat.get)
    } else {
      val timestampFormatString = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
        .CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      new SimpleDateFormat(timestampFormatString)
    }

    val dateFormat = if (specificFormat.isDefined) {
      new SimpleDateFormat(specificFormat.get)
    } else {
      val dateFormatString = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
        .CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
      new SimpleDateFormat(dateFormatString)
    }

    // generate RDD[(K, V)] to use the partitionBy method of PairRDDFunctions
    val inputRDD: RDD[(String, Row)] = if (dataFrame.isDefined) {
      // input data from DataFrame
      val delimiterLevel1 = carbonLoadModel.getComplexDelimiterLevel1
      val delimiterLevel2 = carbonLoadModel.getComplexDelimiterLevel2
      val serializationNullFormat =
        carbonLoadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)
      dataFrame.get.rdd.map { row =>
        if (null != row && row.length > partitionColumnIndex &&
          null != row.get(partitionColumnIndex)) {
          (CarbonScalaUtil.getString(row.get(partitionColumnIndex), serializationNullFormat,
            delimiterLevel1, delimiterLevel2, timeStampFormat, dateFormat), row)
        } else {
          (null, row)
        }
      }
    } else {
      // input data from csv files
      val hadoopConfiguration = new Configuration()
      CommonUtil.configureCSVInputFormat(hadoopConfiguration, carbonLoadModel)
      hadoopConfiguration.set(FileInputFormat.INPUT_DIR, carbonLoadModel.getFactFilePath)
      val columnCount = columns.length
      new NewHadoopRDD[NullWritable, StringArrayWritable](
        sqlContext.sparkContext,
        classOf[CSVInputFormat],
        classOf[NullWritable],
        classOf[StringArrayWritable],
        hadoopConfiguration
      ).map { currentRow =>
        if (null == currentRow || null == currentRow._2) {
          val row = new StringArrayRow(new Array[String](columnCount))
          (null, row)
        } else {
          val row = new StringArrayRow(new Array[String](columnCount))
          val values = currentRow._2.get()
          if (values != null && values.length > partitionColumnIndex) {
            (currentRow._2.get()(partitionColumnIndex), row.setValues(currentRow._2.get()))
          } else {
            (null, row.setValues(currentRow._2.get()))
          }
        }
      }
    }

    val partitioner = PartitionFactory.getPartitioner(partitionInfo)
    if (partitionColumnDataType == DataTypes.STRING) {
      if (partitionInfo.getPartitionType == PartitionType.RANGE) {
        inputRDD.map { row => (ByteUtil.toBytes(row._1), row._2) }
          .partitionBy(partitioner)
          .map(_._2)
      } else {
        inputRDD.partitionBy(partitioner)
          .map(_._2)
      }
    } else {
      inputRDD.map { row =>
        (PartitionUtil.getDataBasedOnDataType(row._1, partitionColumnDataType, timeStampFormat,
          dateFormat), row._2)
      }
        .partitionBy(partitioner)
        .map(_._2)
    }
  }

  private def writeDictionary(carbonLoadModel: CarbonLoadModel,
      result: Option[DictionaryServer], writeAll: Boolean) = {
    // write dictionary file
    val uniqueTableName: String = s"${ carbonLoadModel.getDatabaseName }_${
      carbonLoadModel.getTableName
    }"
    result match {
      case Some(server) =>
        try {
          server.writeTableDictionary(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
            .getCarbonTableIdentifier.getTableId)
        } catch {
          case _: Exception =>
            LOGGER.error(s"Error while writing dictionary file for $uniqueTableName")
            throw new Exception("Dataload failed due to error while writing dictionary file!")
        }
      case _ =>
    }
  }
}
