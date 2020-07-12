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


import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql._
import org.scalatest._

import scala.util.control.NonFatal

//@Ignore
class ReadSuiteV2 extends ReadSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  override def beforeAll() {
    try {

      helper = new TestHelper()
      if (isDebug) {
        log.setLevel(Level.DEBUG)
      }
      helper.init(isDebug, "true")

      // DB
      helper.hiveExecute("DROP DATABASE IF EXISTS "+ DEFAULT_DBNAME +" CASCADE")
      helper.hiveExecute("CREATE DATABASE IF NOT EXISTS "+ DEFAULT_DBNAME)
    } catch {
      case NonFatal(e) => log.info("failed " + e)
    }
  }

  // Test Run
  //readTest(Table.allFullAcidTypes(), insertOnly = false, "v2")
  readTest(Table.allInsertOnlyTypes(), insertOnly = true, "v2")

  //joinTest(Table.allFullAcidTypes(), Table.allFullAcidTypes(), "v2")
  //joinTest(Table.allInsertOnlyTypes(), Table.allFullAcidTypes(), "v2")
  joinTest(Table.allInsertOnlyTypes(), Table.allInsertOnlyTypes(), "v2")

  // Run predicatePushdown test for InsertOnly/FullAcid, Partitioned/NonPartitioned tables
  // It should work in file formats which supports predicate pushdown - orc/parquet
  predicatePushdownTest(List(
    (Table.orcPartitionedInsertOnlyTable, true, true),
    (Table.parquetPartitionedInsertOnlyTable, true, true),
    (Table.textPartitionedInsertOnlyTable, true, false),
    (Table.orcInsertOnlyTable, false, true),
    (Table.parquetInsertOnlyTable, false, true),
    (Table.textInsertOnlyTable, false, false),
    (Table.orcFullACIDTable, false, true),
    (Table.orcPartitionedFullACIDTable, true, true)
  ), "v2")
}
