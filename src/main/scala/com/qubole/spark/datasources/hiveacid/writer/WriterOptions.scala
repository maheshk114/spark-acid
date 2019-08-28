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

package com.qubole.spark.datasources.hiveacid.writer

import com.qubole.shaded.hadoop.hive.ql.plan.FileSinkDesc
import com.qubole.spark.datasources.hiveacid.HiveAcidOperation
import com.qubole.spark.datasources.hiveacid.util.SerializableConfiguration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * Writer options which will be serialized and sent to each executor
 */
private[writer] class WriterOptions(val currentWriteId: Long,
                                    val operationType: HiveAcidOperation.OperationType,
                                    val serializableHadoopConf: SerializableConfiguration,
                                    val dataColumns: Seq[Attribute],
                                    val partitionColumns: Seq[Attribute],
                                    val allColumns: Seq[Attribute],
                                    val timeZoneId: String) extends Serializable

private[writer] class Hive3WriterOptions(val rootPath: String,
                                         val fileSinkDesc: FileSinkDesc) extends Serializable {
  lazy val getFileSinkDesc: FileSinkDesc = {
    fileSinkDesc.setDirName(new Path(rootPath))
    fileSinkDesc
  }
}