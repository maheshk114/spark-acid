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

package com.qubole.spark.hiveacid

import java.lang.String.format
import java.io.IOException
import java.lang.String.format
import java.util.{ArrayList, List, Map}
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import com.qubole.spark.hiveacid.hive.{HiveAcidMetadata, HiveConverter}
import org.apache.spark.SparkContext
import com.qubole.shaded.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.sql.types.StructType
import com.qubole.shaded.hadoop.hive.metastore.api.FieldSchema
import com.qubole.spark.hiveacid.rdd.{HiveAcidPartition, HiveAcidRDD}
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch
import org.apache.hadoop.mapred.InputSplit
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.hadoop.mapred.{FileInputFormat, _}
import com.qubole.shaded.hadoop.hive.ql.io.{AcidInputFormat, AcidUtils, HiveInputFormat, RecordIdentifier}
import com.qubole.spark.hiveacid.reader.hive.HiveAcidReader
import com.qubole.spark.hiveacid.transaction.HiveAcidTxn
import com.qubole.spark.hiveacid.util.{HiveAcidCommon, SerializableConfiguration, Util}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.Writable
import com.qubole.shaded.hadoop.hive.metastore.utils.MetaStoreUtils.{getColumnNamesFromFieldSchema, getColumnTypesFromFieldSchema}
import com.qubole.shaded.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.broadcast.Broadcast
import com.qubole.shaded.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader
import com.qubole.shaded.hadoop.hive.ql.io.orc.OrcSplit
import com.qubole.shaded.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import com.qubole.shaded.orc.OrcFile
import com.qubole.shaded.orc.mapred.OrcInputFormat
import com.qubole.shaded.orc.OrcConf
import com.qubole.shaded.hadoop.hive.ql.io.orc.OrcSplit
import com.qubole.spark.hiveacid.reader.TableReader
import org.apache.spark.sql.vectorized.ColumnVector
//import org.apache.spark.sql.execution.datasources.orc.OrcColumnVector
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader._
import com.qubole.spark.hiveacid.reader.v2.HiveAcidInputPartitionV2


/**
  * Data source V2 implementation for HiveACID
*/
class HiveAcidDataSourceV2Reader
  extends DataSourceV2 with DataSourceReader with SupportsScanColumnarBatch
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters  with Logging {

  def this(options: java.util.Map[String, String],
           sparkSession : SparkSession,
           dbName : String,
           tblName : String) {
    this()
    this.options = options
    this.sparkSession = sparkSession
    this.dbName = dbName
    this.tblName = tblName
    hiveAcidMetadata = HiveAcidMetadata.fromSparkSession(sparkSession, tblName)
    //this is a hack to prevent the following situation:
    //Spark(v 2.4.0) creates one instance of DataSourceReader to call readSchema() and then a new instance of DataSourceReader
    //to call pushFilters(), planBatchInputPartitions() etc. Since it uses different DataSourceReader instances,
    //and reads schema in former instance, schema remains null in the latter instance(which causes problems for other methods).
    //More discussion: http://apache-spark-user-list.1001560.n3.nabble.com/DataSourceV2-APIs-creating-multiple-instances-of-DataSourceReader-and-hence-not-preserving-the-state-tc33646.html
    //Also a null check on schema is already there in readSchema() to prevent initialization more than once just in case.
    readSchema
    //sessionId = getCurrentSessionId
  }

  private var options: java.util.Map[String, String] = null
  private var sparkSession : SparkSession = null
  private var dbName : String = null
  private var tblName : String = null

  private var curTxn: HiveAcidTxn = _

  //The pruned schema
  private var schema: StructType = null

  private var pushedFilterArray : Array[Filter] = null

  private var hiveAcidMetadata: HiveAcidMetadata = _

  private lazy val client: HiveMetaStoreClient =
    new HiveMetaStoreClient(SparkContext.getOrCreate.hadoopConfiguration, null, false)

  private def convertSchema(fieldSchemas: java.util.List[FieldSchema]): StructType = {
    val types = new java.util.ArrayList[String]
    import scala.collection.JavaConversions._
    for (fieldSchema <- fieldSchemas) {
      types.add(format("`%s` %s", fieldSchema.getName(), fieldSchema.getType().toString))
    }
    StructType.fromDDL(String.join(", ", types))
  }

  override def readSchema: StructType = {
    if (schema == null) {
      schema = hiveAcidMetadata.tableSchema
    }
    schema
  }

  override def planBatchInputPartitions() : java.util.List[InputPartition[ColumnarBatch]] = {
    val factories = new java.util.ArrayList[InputPartition[ColumnarBatch]]
    inTxn {
      import scala.collection.JavaConversions._
      val reader = new TableReader(sparkSession, curTxn, hiveAcidMetadata)
      val hiveReader = reader.getReader(schema.fieldNames,
        pushedFilterArray, new SparkAcidConf(sparkSession, options.toMap))
      factories.addAll(hiveReader)
    }
    factories
  }

  def endTxn(): Unit = if (curTxn != null && !curTxn.isClosed.get()) {
    curTxn.end()
    curTxn = null
  }

  // Start local transaction if not passed.
  private def getOrCreateTxn(): Unit = {
    // The same relation can be used to create multiple rdds. Each of the rdd will have same txn snapshot.
    if (curTxn != null) {
      logInfo(s"active txn found for hive table $curTxn")
      return
    }

    curTxn = HiveAcidTxn.createTransaction(sparkSession)
    curTxn.begin()
    logDebug(s"Started normal Transactions $curTxn")
  }

  // End and reset transaction and snapshot
  // if locally started
  private def unsetOrEndTxn(abort: Boolean = false): Unit = {
    curTxn.end(abort)
    curTxn = null
  }

  // Start and end transaction under protection.
  private def inTxn(f: => Unit): Unit = synchronized {
    getOrCreateTxn()
    var abort = false
    try { f }
    catch {
      case e: Exception =>
        logError("Unable to execute in transactions due to: " + e.getMessage)
        abort = true;
    }
    finally {
      unsetOrEndTxn(abort)
    }
  }

  override def pushFilters (filter: Array[Filter]): Array[Filter] = {
    this.pushedFilterArray = filter
    filter
  }

  override def pushedFilters(): Array[Filter] = this.pushedFilterArray

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.schema = requiredSchema
  }
}