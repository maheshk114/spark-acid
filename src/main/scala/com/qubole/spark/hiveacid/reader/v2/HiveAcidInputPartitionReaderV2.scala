package com.qubole.spark.hiveacid.reader.v2

import java.io.IOException
import java.util._
import java.util.List
import scala.collection.JavaConverters._

import com.qubole.spark.hiveacid.rdd.HiveAcidPartition
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputFormat
import com.qubole.shaded.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader
import org.apache.hadoop.mapred.JobConf
import com.qubole.spark.hiveacid.util.{HiveAcidCommon, SerializableConfiguration}
import org.apache.spark.broadcast.Broadcast
import com.qubole.shaded.orc.TypeDescription
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.execution.datasources.orc._
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.mapred.JobID
import org.apache.spark.TaskContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst._
import com.qubole.shaded.hadoop.hive.serde2.ColumnProjectionUtils
import com.qubole.shaded.orc.OrcFile
import com.qubole.shaded.orc.OrcConf
import com.qubole.shaded.hadoop.hive.ql.io.orc.OrcSplit

private[v2] class HiveAcidInputPartitionReaderV2(split: HiveAcidPartition,
                                                 broadcastedConf: Broadcast[SerializableConfiguration],
                                                 partitionValues : InternalRow,
                                                 requiredFields: Array[StructField],
                                                 partitionSchema : StructType)
        extends InputPartitionReader[ColumnarBatch] /*with Logging */{
  private val jobConf : JobConf = HiveAcidCommon.getJobConf(broadcastedConf.value.value, "test")
  private val sparkOrcColReader : OrcColumnarBatchReader =
    new OrcColumnarBatchReader(true, false, 1024)

  private def initReader() : Unit = {
    // Get the reader schema using the column names and types set in hive conf.
    val readerSchema: TypeDescription =
      com.qubole.shaded.hadoop.hive.ql.io.orc.OrcInputFormat.getDesiredRowTypeDescr(jobConf, true, 2147483647)

    // Set it as orc.mapred.input.schema so that the reader will read only the required columns
    jobConf.set("orc.mapred.input.schema", readerSchema.toString)

    val fileSplit = split.inputSplit.value.asInstanceOf[OrcSplit]
    val readerLocal = OrcFile.createReader(fileSplit.getPath,
      OrcFile.readerOptions(jobConf).maxLength(
        OrcConf.MAX_FILE_LENGTH.getLong(jobConf)).filesystem(fileSplit.getPath.getFileSystem(jobConf)))

    // Get the column id from hive conf para. TODO : Can be sent via a parameter
    val colIds = jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR)
    val requestedColIds = if (!colIds.isEmpty()) {
      colIds.split(",").map(a => a.toInt)
    } else {
      Array[Int]()
    }

    // Register the listener for closing the reader before init is done.
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val taskAttemptContext = new org.apache.hadoop.mapred.TaskAttemptContextImpl(jobConf, attemptId)
    val iter = new org.apache.spark.sql.execution.datasources.RecordReaderIterator(sparkOrcColReader)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))

    sparkOrcColReader.initialize(fileSplit, taskAttemptContext)
    sparkOrcColReader.initBatch(readerLocal.getSchema, requestedColIds,
      requiredFields, partitionSchema, partitionValues)
  }
  initReader()

  @throws(classOf[IOException])
  override def next() : Boolean = {
    sparkOrcColReader.nextKeyValue()
  }

  override def get () : ColumnarBatch = {
    sparkOrcColReader.getCurrentValue
  }

  @throws(classOf[IOException])
  override def close() : Unit = {
    sparkOrcColReader.close()
  }
}
