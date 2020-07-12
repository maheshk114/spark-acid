/*
 * Copyright 2019 Qubole, Inc.  All rights reserved.
 *
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

package com.qubole.spark.hiveacid.util

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Path
import com.qubole.shaded.hadoop.hive.common.ValidTxnList
import org.apache.hadoop.mapred.{FileInputFormat, _}
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.util.ReflectionUtils
import com.qubole.shaded.hadoop.hive.common.ValidWriteIdList
import com.qubole.shaded.hadoop.hive.ql.io.{AcidInputFormat, AcidUtils, HiveInputFormat, RecordIdentifier}
import com.qubole.spark.hiveacid.rdd.{Cache, HiveAcidPartition}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.io.Writable
import org.apache.spark.Partition

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import com.qubole.shaded.hadoop.hive.ql.exec.vector.VectorizedRowBatch

object HiveAcidCommon  extends Logging {

  private val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  private object Cache {
    val jobConf =  new ConcurrentHashMap[String, Any]()
  }

  private def getCachedMetadata(key: String): Any = {
    Cache.jobConf.get(key)
  }

  private def putCachedMetadata(key: String, value: Any): Unit = {
    Cache.jobConf.put(key, value)
  }

  def getJobConf(conf: Configuration,
                 jobConfCacheKey: String,
                 shouldCloneJobConf: Boolean = false,
                 initLocalJobConfFuncOpt: Option[JobConf => Unit] = None) : JobConf = {
   if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546).  This problem occurs
      // somewhat rarely because most jobs treat the configuration as though it's immutable.  One
      // solution, implemented here, is to clone the Configuration object.  Unfortunately, this
      // clone can be very expensive.  To avoid unexpected performance regressions for workloads and
      // Hadoop versions that do not suffer from these thread-safety issues, this cloning is
      // disabled by default.
      CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        val newJobConf = new JobConf(conf)
        if (!conf.isInstanceOf[JobConf]) {
          initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
        }
        newJobConf
      }
    } else {
      conf match {
        case c: JobConf =>
          logDebug("Re-using user-broadcasted JobConf")
          c
        case _ =>
          Option(getCachedMetadata(jobConfCacheKey))
            .map { conf =>
              logDebug("Re-using cached JobConf")
              conf.asInstanceOf[JobConf]
            }
            .getOrElse {
              // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in
              // the local process. The local cache is accessed through putCachedMetadata().
              // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary
              // objects. Synchronize to prevent ConcurrentModificationException (SPARK-1097,
              // HADOOP-10456).
              CONFIGURATION_INSTANTIATION_LOCK.synchronized {
                logDebug("Creating new JobConf and caching it for later re-use")
                val newJobConf = new JobConf(conf)
                initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
                putCachedMetadata(jobConfCacheKey, newJobConf)
                newJobConf
              }
            }
      }
    }
  }

  def getInputFormat[K, V](conf: JobConf, inputFormatClass: Class[_ <: InputFormat[K, V]]):
      InputFormat[K, V] = {
    val newInputFormat = ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
    newInputFormat match {
      case c: Configurable => c.setConf(conf)
      case _ =>
    }
    newInputFormat
  }

  def getInputSplits[K, V](jobConf : JobConf,
                     validWriteIds: ValidWriteIdList,
                     rddId: Int,
                     isFullAcidTable: Boolean,
                     inputFormatClass: Class[_ <: InputFormat[K, V]],
                     minPartitions: Int = 0,
                     ignoreEmptySplits : Boolean = true,
                     ignoreMissingFiles : Boolean = false) : Array[Partition] = {
    var jobConfLocal = jobConf
    if (isFullAcidTable) {
      // If full ACID table, just set the right writeIds, the
      // OrcInputFormat.getSplits() will take care of the rest
      AcidUtils.setValidWriteIdList(jobConfLocal, validWriteIds)
    } else {
      val finalPaths = new ListBuffer[Path]()
      val pathsWithFileOriginals = new ListBuffer[Path]()
      val dirs = FileInputFormat.getInputPaths(jobConfLocal).toSeq // Seq(acidState.location)
      HiveInputFormat.processPathsForMmRead(dirs, jobConfLocal, validWriteIds,
        finalPaths, pathsWithFileOriginals)

      if (finalPaths.nonEmpty) {
        FileInputFormat.setInputPaths(jobConfLocal, finalPaths.toList: _*)
        // Need recursive to be set to true because MM Tables can have a directory structure like:
        // ~/warehouse/hello_mm/base_0000034/HIVE_UNION_SUBDIR_1/000000_0
        // ~/warehouse/hello_mm/base_0000034/HIVE_UNION_SUBDIR_2/000000_0
        // ~/warehouse/hello_mm/delta_0000033_0000033_0001/HIVE_UNION_SUBDIR_1/000000_0
        // ~/warehouse/hello_mm/delta_0000033_0000033_0002/HIVE_UNION_SUBDIR_2/000000_0
        // ... which is created on UNION ALL operations
        jobConfLocal.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true)
      }

      if (pathsWithFileOriginals.nonEmpty) {
        // We are going to add splits for these directories with recursive = false, so we ignore
        // any subdirectories (deltas or original directories) and only read the original files.
        // The fact that there's a loop calling addSplitsForGroup already implies it's ok to
        // the real input format multiple times... however some split concurrency/etc configs
        // that are applied separately in each call will effectively be ignored for such splits.
        jobConfLocal = HiveInputFormat.createConfForMmOriginalsSplit(jobConfLocal, pathsWithFileOriginals)
      }

    }
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConfLocal)
    try {
      val allInputSplits = getInputFormat(jobConfLocal, inputFormatClass).getSplits(jobConfLocal, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new HiveAcidPartition(rddId, i, inputSplits(i))
        logWarning("getPartitions : Input split: " + inputSplits(i))
      }
      array
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        val inputDir = jobConfLocal.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR)
        logWarning(s"$inputDir doesn't exist and no" +
          s" partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }
}
