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

package org.apache.spark.rpc

import java.io.IOException
import java.net.InetAddress
import java.util.{List => JList, Map => JMap, Objects, Random, Set => JSet, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SecurityManager, SerializableWritable, SparkConf}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.search._
import org.apache.spark.util.ThreadUtils

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.block.Distributable
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.store.worker.Status

/**
 * Master of CarbonSearch.
 * It listens to [[Master.port]] to wait for worker to register.
 * And it provides search API to fire RPC call to workers.
 */
@InterfaceAudience.Internal
class Master(sparkConf: SparkConf, port: Int) {
  private val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  // worker hostname map to EndpointRef
  private val workers = mutable.Map[String, RpcEndpointRef]()

  private val random = new Random

  private var rpcEnv: RpcEnv = _

  def this(sparkConf: SparkConf) = {
    this(sparkConf, Master.DEFAULT_PORT)
  }

  /** start service and listen on port passed in constructor */
  def startService(): Unit = {
    if (rpcEnv == null) {
      new Thread(new Runnable {
        override def run(): Unit = {
          val hostname = InetAddress.getLocalHost.getHostName
          val config = new RpcEnvConfig(
            sparkConf, "registry-service", hostname, "", Master.DEFAULT_PORT,
            new SecurityManager(sparkConf), false)
          rpcEnv = new NettyRpcEnvFactory().create(config)
          val registryEndpoint: RpcEndpoint = new Registry(rpcEnv, Master.this)
          rpcEnv.setupEndpoint("registry-service", registryEndpoint)
          rpcEnv.awaitTermination()
        }
      }).start()
    }
  }

  def stopService(): Unit = {
    if (rpcEnv != null) {
      rpcEnv.shutdown()
      rpcEnv = null
    }
  }

  def stopAllWorkers(): Unit = {
    val futures = workers.mapValues { ref =>
      ref.ask[ShutdownResponse](ShutdownRequest("user"))
    }
    futures.foreach { case (hostname, future) =>
      ThreadUtils.awaitResult(future, Duration.apply("10s"))
      future.value match {
        case Some(result) =>
          result match {
            case Success(response) => workers.remove(hostname)
            case Failure(throwable) => throw new IOException(throwable.getMessage)
          }
        case None => throw new ExecutionTimeoutException
      }
    }
  }

  /** A new searcher is trying to register, add it to the map and connect to this searcher */
  def addWorker(request: RegisterWorkerRequest): RegisterWorkerResponse = {
    LOG.info(s"Receive Register request from worker ${request.hostname}:${request.port} " +
             s"with ${request.cores} cores")
    val workerId = UUID.randomUUID().toString
    val workerHostname = request.hostname
    val workerPort = request.port
    LOG.info(s"connecting to worker ${request.hostname}:${request.port}, workerId $workerId")

    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(
      RpcAddress(workerHostname, workerPort), "search-service")

    workers.put(workerHostname, endPointRef)
    LOG.info(s"worker ${request.hostname}:${request.port} added")
    RegisterWorkerResponse(workerId)
  }

  private def getEndpoint(workerIP: String) = workers(workerIP)

  /**
   * Execute search by firing RPC call to worker, return the result rows
   */
  def search(table: CarbonTable, columns: Array[String], filter: Expression): Array[CarbonRow] = {
    Objects.requireNonNull(table)
    Objects.requireNonNull(columns)
    if (workers.isEmpty) throw new IOException("No worker is available")

    val queryId = random.nextInt
    // prune data and get a mapping of worker hostname to list of blocks,
    // then add these blocks to the SearchRequest and fire the RPC call
    val nodeBlockMapping: JMap[String, JList[Distributable]] = pruneBlock(table, columns, filter)
    val futures = nodeBlockMapping.asScala.map { case (workerIP, blocks) =>
      // Build a SearchRequest
      val split = new SerializableWritable[CarbonMultiBlockSplit](
        new CarbonMultiBlockSplit(blocks, workerIP))
      val request = SearchRequest(queryId, split, table.getTableInfo, columns, filter)

      // fire RPC to worker asynchronously
      getEndpoint(workerIP).ask[SearchResult](request)
    }
    // get all results from RPC response and return to caller
    val output = new ArrayBuffer[CarbonRow]
    futures.foreach { future: Future[SearchResult] =>
      ThreadUtils.awaitResult(future, Duration.apply("10s"))
      future.value match {
        case Some(response: Try[SearchResult]) =>
          response match {
            case Success(result) =>
              if (result.queryId != queryId) {
                throw new IOException(
                  s"queryId in response does not match request: ${ result.queryId } != $queryId")
              }
              if (result.status != Status.SUCCESS.ordinal()) {
                throw new IOException(s"failure in worker: ${ result.message }")
              }

              val itor = result.rows.iterator
              while (itor.hasNext) {
                output += new CarbonRow(itor.next())
              }
            case Failure(e) =>
              throw new IOException(s"exception in worker: ${e.getMessage}")
          }
        case None =>
          throw new ExecutionTimeoutException()
      }
    }
    output.toArray
  }

  /**
   * Prune data by using CarbonInputFormat.getSplit
   * Return a mapping of hostname to list of block
   */
  private def pruneBlock(
      table: CarbonTable,
      columns: Array[String],
      filter: Expression): JMap[String, JList[Distributable]] = {
    val jobConf = new JobConf(new Configuration)
    val job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonTableInputFormat(
      job, table, columns, filter, null, null)
    val splits = format.getSplits(job)
    val distributables = splits.asScala.map { split =>
      split.asInstanceOf[Distributable]
    }
    CarbonLoaderUtil.nodeBlockMapping(
      distributables.asJava,
      -1,
      workers.keySet.toList.asJava,
      CarbonLoaderUtil.BlockAssignmentStrategy.BLOCK_NUM_FIRST)
  }

  /** return hostname of all workers */
  def getWorkers: JSet[String] = workers.keySet.asJava
}

object Master {
  val DEFAULT_PORT = 10020
}

// Exception if execution timed out in search mode
class ExecutionTimeoutException extends RuntimeException
