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

package com.qubole.spark.hiveacid.reader.v2;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import com.qubole.spark.hiveacid.util.HiveAcidCommon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.qubole.shaded.orc.OrcConf;
import com.qubole.shaded.orc.OrcFile;
import com.qubole.shaded.orc.Reader;
import com.qubole.shaded.orc.TypeDescription;
import com.qubole.shaded.orc.mapred.OrcInputFormat;
import com.qubole.shaded.hadoop.hive.common.type.HiveDecimal;
import com.qubole.shaded.hadoop.hive.ql.exec.vector.*;
import com.qubole.shaded.hadoop.hive.serde2.io.HiveDecimalWritable;
import com.qubole.shaded.hadoop.hive.ql.io.orc.OrcSplit;
import com.qubole.shaded.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader;
import com.qubole.shaded.hadoop.hive.ql.plan.MapWork;
import com.qubole.shaded.hadoop.hive.ql.exec.Utilities;
import com.qubole.shaded.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.NullWritable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import com.qubole.shaded.hadoop.hive.ql.io.sarg.*;

/**
 * To support vectorization in WholeStageCodeGen, this reader returns ColumnarBatch.
 * After creating, `initialize` and `initBatch` should be called sequentially.
 */
public class OrcColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  // The capacity of vectorized batch.
  private int capacity;

  // Vectorized ORC Row Batch
  private VectorizedRowBatch batch;

  /**
   * The column IDs of the physical ORC file schema which are required by this reader.
   * -1 means this required column doesn't exist in the ORC file.
   */
  private int[] requestedColIds;

  // Record reader from ORC row batch.
  private com.qubole.shaded.orc.RecordReader baseRecordReader;

  private VectorizedOrcAcidRowBatchReader fullAcidRecordReader;

  private StructField[] requiredFields;

  // The result columnar batch for vectorized execution by whole-stage codegen.
  private ColumnarBatch columnarBatch;

  // Writable column vectors of the result columnar batch.
  private WritableColumnVector[] columnVectors;

  // The wrapped ORC column vectors. It should be null if `copyToSpark` is true.
  private org.apache.spark.sql.vectorized.ColumnVector[] orcVectorWrappers;

  private OrcSplit fileSplit;

  private Configuration conf;

  private int rootCol;

  public OrcColumnarBatchReader(int capacity) {
    this.capacity = capacity;
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public ColumnarBatch getCurrentValue() {
    return columnarBatch;
  }

  @Override
  public float getProgress() throws IOException {
    return fullAcidRecordReader.getProgress();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    return nextBatch();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    if (fullAcidRecordReader != null) {
      fullAcidRecordReader.close();
      fullAcidRecordReader = null;
    }
    if (baseRecordReader != null) {
      baseRecordReader.close();
      baseRecordReader = null;
    }
  }

  private String[] getSargColumnNames(String[] originalColumnNames,
                                      List<com.qubole.shaded.orc.OrcProto.Type> types,
                                      boolean[] includedColumns, boolean isOriginal) {
    String[] columnNames = new String[types.size() - rootCol];
    int i = 0;
    Iterator var7 = ((com.qubole.shaded.orc.OrcProto.Type)types.get(rootCol)).getSubtypesList().iterator();

    while(true) {
      int columnId;
      do {
        if (!var7.hasNext()) {
          return columnNames;
        }

        columnId = (Integer)var7.next();
      } while(includedColumns != null && !includedColumns[columnId - rootCol]);

      columnNames[columnId - rootCol] = originalColumnNames[i++];
    }
  }

  private void setSearchArgument(Reader.Options options, List<com.qubole.shaded.orc.OrcProto.Type> types, Configuration conf) {
    String neededColumnNames = conf.get("hive.io.file.readcolumn.names");
    if (neededColumnNames == null) {
      //LOG.debug("No ORC pushdown predicate - no column names");
      options.searchArgument((SearchArgument)null, (String[])null);
    } else {
      SearchArgument sarg = com.qubole.shaded.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg.createFromConf(conf);
      if (sarg == null) {
        //LOG.debug("No ORC pushdown predicate");
        options.searchArgument((SearchArgument)null, (String[])null);
      } else {
        //if (LOG.isInfoEnabled()) {
         // LOG.info("ORC pushdown predicate: " + sarg);
       // }

        options.searchArgument(sarg,
                getSargColumnNames(neededColumnNames.split(","), types, options.getInclude(), true));
      }
    }
  }

  private Reader.Options setSearchArgumentForOption(Configuration conf,
                                                TypeDescription readerSchema,
                                                    Reader.Options readerOptions) throws IOException {
    //Reader.Options readerOptions = new Reader.Options(conf).schema(readerSchema);
    // TODO: Convert genIncludedColumns and setSearchArgument to use TypeDescription.
    final List<com.qubole.shaded.orc.OrcProto.Type> schemaTypes =
            com.qubole.shaded.orc.OrcUtils.getOrcTypes(readerSchema);
    //readerOptions.include(com.qubole.shaded.hadoop.hive.ql.io.orc.OrcInputFormat.genIncludedColumns(readerSchema,
           // com.qubole.shaded.hadoop.hive.serde2.ColumnProjectionUtils.getReadColumnIDs(conf)));
    //todo: last param is bogus. why is this hardcoded?
    setSearchArgument(readerOptions, schemaTypes, conf);
    return readerOptions;
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `initBatch` is needed to be called after this.
   */
  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext) throws IOException {
    fileSplit = (OrcSplit)inputSplit;
    conf = taskAttemptContext.getConfiguration();
  }

  private VectorizedOrcAcidRowBatchReader initHiveAcidReader(Configuration conf, OrcSplit orcSplit,
                                                             com.qubole.shaded.orc.RecordReader innerReader) {
    conf.set("hive.vectorized.execution.enabled", "true");
    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    Utilities.setMapWork(conf, mapWork);

    org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> baseReader
            = new org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch>() {

      @Override
      public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
        return innerReader.nextBatch(value);
      }

      @Override
      public NullWritable createKey() {
        return NullWritable.get();
      }

      @Override
      public VectorizedRowBatch createValue() {
        return batch;
      }

      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public void close() throws IOException {
        innerReader.close();
      }

      @Override
      public float getProgress() throws IOException {
        return innerReader.getProgress();
      }
    };

    try {
      return new VectorizedOrcAcidRowBatchReader(orcSplit, new JobConf(conf), Reporter.NULL, baseReader, rbCtx, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Initialize columnar batch by setting required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   */
  public void initBatch(
          TypeDescription orcSchema,
          int[] requestedColIds,
          StructField[] requiredFields,
          StructType partitionSchema,
          InternalRow partitionValues,
          boolean isFullAcidTable) throws IOException {

    if (!isFullAcidTable) {
      //rootCol = org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.getRootColumn(true);
      rootCol = 0;
    } else {
      //rootCol = org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.getRootColumn(false) - 1;
      // In ORC, for full ACID table, the first 5 fields stores the transaction metadata.
      rootCol = 5;
    }

    Reader readerInner = OrcFile.createReader(
            fileSplit.getPath(), OrcFile.readerOptions(conf)
                    .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf))
                    .filesystem(fileSplit.getPath().getFileSystem(conf)));
    Reader.Options options = /*createOptionsForReader(conf, orcSchema);*/
            OrcInputFormat.buildOptions(conf, readerInner, fileSplit.getStart(), fileSplit.getLength());
    setSearchArgumentForOption(conf, orcSchema, options);
    baseRecordReader = readerInner.rows(options);

    batch = orcSchema.createRowBatch(capacity);
    assert(!batch.selectedInUse); // `selectedInUse` should be initialized with `false`.

    this.requiredFields = requiredFields;
    this.requestedColIds = requestedColIds;
    assert(requiredFields.length == requestedColIds.length);

    StructType resultSchema = new StructType(requiredFields);
    for (StructField f : partitionSchema.fields()) {
      resultSchema = resultSchema.add(f);
    }

    ColumnVector[] fields;
    orcVectorWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];
    if (rootCol == 0) {
      fields  = batch.cols;
    } else {
      fields  = ((StructColumnVector)batch.cols[rootCol]).fields;
    }

    if (isFullAcidTable) {
      /*if (MEMORY_MODE == MemoryMode.OFF_HEAP) {
        columnVectors = OffHeapColumnVector.allocateColumns(capacity, resultSchema);
      } else {*/
        columnVectors = OnHeapColumnVector.allocateColumns(capacity, resultSchema);
      //}

      // Initialize the missing columns once.
      for (int i = 0; i < requiredFields.length; i++) {
        if (requestedColIds[i] == -1) {
          columnVectors[i].putNulls(0, capacity);
          columnVectors[i].setIsConstant();
        }
      }

      if (partitionValues.numFields() > 0) {
        int partitionIdx = requiredFields.length;
        for (int i = 0; i < partitionValues.numFields(); i++) {
          ColumnVectorUtils.populate(columnVectors[i + partitionIdx], partitionValues, i);
          columnVectors[i + partitionIdx].setIsConstant();
        }
      }
    }

    // Just wrap the ORC column vector instead of copying it to Spark column vector.
    orcVectorWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];
    //StructColumnVector dataCols  = (StructColumnVector)batch.cols[5];
    for (int i = 0; i < requiredFields.length; i++) {
      DataType dt = requiredFields[i].dataType();
      int colId = requestedColIds[i];
      // Initialize the missing columns once.
      if (colId == -1) {
        OnHeapColumnVector missingCol = new OnHeapColumnVector(capacity, dt);
        missingCol.putNulls(0, capacity);
        missingCol.setIsConstant();
        orcVectorWrappers[i] = missingCol;
      } else {
        orcVectorWrappers[i] = new OrcColumnVector(dt, fields[colId]);
      }
    }

    if (partitionValues.numFields() > 0) {
      int partitionIdx = requiredFields.length;
      for (int i = 0; i < partitionValues.numFields(); i++) {
        DataType dt = partitionSchema.fields()[i].dataType();
        OnHeapColumnVector partitionCol = new OnHeapColumnVector(capacity, dt);
        ColumnVectorUtils.populate(partitionCol, partitionValues, i);
        partitionCol.setIsConstant();
        orcVectorWrappers[partitionIdx + i] = partitionCol;
      }
    }

    if (isFullAcidTable) {
      fullAcidRecordReader = initHiveAcidReader(conf, fileSplit, baseRecordReader);
    } else {
      fullAcidRecordReader = null;
    }
  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  private boolean nextBatch() throws IOException {
    if (fullAcidRecordReader != null) {
      fullAcidRecordReader.next(NullWritable.get(), batch);
    } else {
      baseRecordReader.nextBatch(batch);
    }

    //recordReader.nextBatch(batch);
    int batchSize = batch.size;
    if (batchSize == 0) {
      return false;
    }

    if (!batch.selectedInUse) {
      for (int i = 0; i < requiredFields.length; i++) {
        if (requestedColIds[i] != -1) {
          ((OrcColumnVector) orcVectorWrappers[i]).setBatchSize(batchSize);
        }
      }
      columnarBatch = new ColumnarBatch(orcVectorWrappers);
      columnarBatch.setNumRows(batchSize);
      return true;
    }

    for (WritableColumnVector vector : columnVectors) {
      vector.reset();
    }

    StructColumnVector dataCols  = (StructColumnVector)batch.cols[rootCol];
    for (int i = 0; i < requiredFields.length; i++) {
      StructField field = requiredFields[i];
      WritableColumnVector toColumn = columnVectors[i];

      if (requestedColIds[i] >= 0) {
        ColumnVector fromColumn = dataCols.fields[requestedColIds[i]];

        if (fromColumn.isRepeating) {
          putRepeatingValues(batchSize, field, fromColumn, toColumn);
        } else if (fromColumn.noNulls) {
          putNonNullValues(batchSize, field, fromColumn, toColumn, batch.selected);
        } else {
          putValues(batchSize, field, fromColumn, toColumn, batch.selected);
        }
      }
    }
    columnarBatch = new ColumnarBatch(columnVectors);
    columnarBatch.setNumRows(batchSize);
    return true;
  }

  private void putRepeatingValues(
          int batchSize,
          StructField field,
          ColumnVector fromColumn,
          WritableColumnVector toColumn) {
    if (fromColumn.isNull[0]) {
      toColumn.putNulls(0, batchSize);
    } else {
      DataType type = field.dataType();
      if (type instanceof BooleanType) {
        toColumn.putBooleans(0, batchSize, ((LongColumnVector)fromColumn).vector[0] == 1);
      } else if (type instanceof ByteType) {
        toColumn.putBytes(0, batchSize, (byte)((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof ShortType) {
        toColumn.putShorts(0, batchSize, (short)((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof IntegerType || type instanceof DateType) {
        toColumn.putInts(0, batchSize, (int)((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof LongType) {
        toColumn.putLongs(0, batchSize, ((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof TimestampType) {
        toColumn.putLongs(0, batchSize,
                fromTimestampColumnVector((TimestampColumnVector)fromColumn, 0));
      } else if (type instanceof FloatType) {
        toColumn.putFloats(0, batchSize, (float)((DoubleColumnVector)fromColumn).vector[0]);
      } else if (type instanceof DoubleType) {
        toColumn.putDoubles(0, batchSize, ((DoubleColumnVector)fromColumn).vector[0]);
      } else if (type instanceof StringType || type instanceof BinaryType) {
        BytesColumnVector data = (BytesColumnVector)fromColumn;
        int size = data.vector[0].length;
        toColumn.arrayData().reserve(size);
        toColumn.arrayData().putBytes(0, size, data.vector[0], 0);
        for (int index = 0; index < batchSize; index++) {
          toColumn.putArray(index, 0, size);
        }
      } else if (type instanceof DecimalType) {
        DecimalType decimalType = (DecimalType)type;
        putDecimalWritables(
                toColumn,
                batchSize,
                decimalType.precision(),
                decimalType.scale(),
                ((DecimalColumnVector)fromColumn).vector[0]);
      } else {
        throw new UnsupportedOperationException("Unsupported Data Type: " + type);
      }
    }
  }

  private void putNonNullValues(
          int batchSize,
          StructField field,
          ColumnVector fromColumn,
          WritableColumnVector toColumn,
          int[] selected) {
    DataType type = field.dataType();
    if (type instanceof BooleanType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putBoolean(index, data[logicalIdx] == 1);
      }
    } else if (type instanceof ByteType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putByte(index, (byte)data[logicalIdx]);
      }
    } else if (type instanceof ShortType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putShort(index, (short)data[logicalIdx]);
      }
    } else if (type instanceof IntegerType || type instanceof DateType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putInt(index, (int)data[logicalIdx]);
      }
    } else if (type instanceof LongType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putLong(index, data[logicalIdx]);
      }
      //toColumn.putLongs(0, batchSize, ((LongColumnVector)fromColumn).vector, 0);
    } else if (type instanceof TimestampType) {
      TimestampColumnVector data = ((TimestampColumnVector)fromColumn);
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putLong(index, fromTimestampColumnVector(data, logicalIdx));
      }
    } else if (type instanceof FloatType) {
      double[] data = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putFloat(index, (float)data[logicalIdx]);
      }
    } else if (type instanceof DoubleType) {
      //toColumn.putDoubles(0, batchSize, ((DoubleColumnVector)fromColumn).vector, 0);
      double[] data = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putDouble(index, data[logicalIdx]);
      }
    } else if (type instanceof StringType || type instanceof BinaryType) {
      BytesColumnVector data = ((BytesColumnVector)fromColumn);
      WritableColumnVector arrayData = toColumn.arrayData();
      int totalNumBytes = IntStream.of(data.length).sum();
      arrayData.reserve(totalNumBytes);
      for (int index = 0, pos = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        arrayData.putBytes(pos, data.length[logicalIdx], data.vector[logicalIdx], data.start[logicalIdx]);
        toColumn.putArray(index, pos, data.length[logicalIdx]);
        pos += data.length[logicalIdx];
      }
    } else if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType)type;
      DecimalColumnVector data = ((DecimalColumnVector)fromColumn);
      if (decimalType.precision() > Decimal.MAX_LONG_DIGITS()) {
        toColumn.arrayData().reserve(batchSize * 16);
      }
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        putDecimalWritable(
                toColumn,
                index,
                decimalType.precision(),
                decimalType.scale(),
                data.vector[logicalIdx]);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Data Type: " + type);
    }
  }

  private void putValues(
          int batchSize,
          StructField field,
          ColumnVector fromColumn,
          WritableColumnVector toColumn,
          int[] selected) {
    DataType type = field.dataType();
    if (type instanceof BooleanType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putBoolean(index, vector[logicalIdx] == 1);
        }
      }
    } else if (type instanceof ByteType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putByte(index, (byte)vector[logicalIdx]);
        }
      }
    } else if (type instanceof ShortType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putShort(index, (short)vector[logicalIdx]);
        }
      }
    } else if (type instanceof IntegerType || type instanceof DateType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putInt(index, (int)vector[logicalIdx]);
        }
      }
    } else if (type instanceof LongType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putLong(index, vector[logicalIdx]);
        }
      }
    } else if (type instanceof TimestampType) {
      TimestampColumnVector vector = ((TimestampColumnVector)fromColumn);
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putLong(index, fromTimestampColumnVector(vector, logicalIdx));
        }
      }
    } else if (type instanceof FloatType) {
      double[] vector = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putFloat(index, (float)vector[logicalIdx]);
        }
      }
    } else if (type instanceof DoubleType) {
      double[] vector = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putDouble(index, vector[logicalIdx]);
        }
      }
    } else if (type instanceof StringType || type instanceof BinaryType) {
      BytesColumnVector vector = (BytesColumnVector)fromColumn;
      WritableColumnVector arrayData = toColumn.arrayData();
      int totalNumBytes = IntStream.of(vector.length).sum();
      arrayData.reserve(totalNumBytes);
      for (int index = 0, pos = 0; index < batchSize; pos += vector.length[index], index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          arrayData.putBytes(pos, vector.length[logicalIdx], vector.vector[logicalIdx], vector.start[logicalIdx]);
          toColumn.putArray(index, pos, vector.length[logicalIdx]);
        }
      }
    } else if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType)type;
      HiveDecimalWritable[] vector = ((DecimalColumnVector)fromColumn).vector;
      if (decimalType.precision() > Decimal.MAX_LONG_DIGITS()) {
        toColumn.arrayData().reserve(batchSize * 16);
      }
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          putDecimalWritable(
                  toColumn,
                  index,
                  decimalType.precision(),
                  decimalType.scale(),
                  vector[logicalIdx]);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Data Type: " + type);
    }
  }

  /**
   * Returns the number of micros since epoch from an element of TimestampColumnVector.
   */
  private static long fromTimestampColumnVector(TimestampColumnVector vector, int index) {
    return vector.time[index] * 1000 + (vector.nanos[index] / 1000 % 1000);
  }

  /**
   * Put a `HiveDecimalWritable` to a `WritableColumnVector`.
   */
  private static void putDecimalWritable(
          WritableColumnVector toColumn,
          int index,
          int precision,
          int scale,
          HiveDecimalWritable decimalWritable) {
    HiveDecimal decimal = decimalWritable.getHiveDecimal();
    Decimal value =
            Decimal.apply(decimal.bigDecimalValue(), decimal.precision(), decimal.scale());
    value.changePrecision(precision, scale);

    if (precision <= Decimal.MAX_INT_DIGITS()) {
      toColumn.putInt(index, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      toColumn.putLong(index, value.toUnscaledLong());
    } else {
      byte[] bytes = value.toJavaBigDecimal().unscaledValue().toByteArray();
      toColumn.arrayData().putBytes(index * 16, bytes.length, bytes, 0);
      toColumn.putArray(index, index * 16, bytes.length);
    }
  }

  /**
   * Put `HiveDecimalWritable`s to a `WritableColumnVector`.
   */
  private static void putDecimalWritables(
          WritableColumnVector toColumn,
          int size,
          int precision,
          int scale,
          HiveDecimalWritable decimalWritable) {
    HiveDecimal decimal = decimalWritable.getHiveDecimal();
    Decimal value =
            Decimal.apply(decimal.bigDecimalValue(), decimal.precision(), decimal.scale());
    value.changePrecision(precision, scale);

    if (precision <= Decimal.MAX_INT_DIGITS()) {
      toColumn.putInts(0, size, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      toColumn.putLongs(0, size, value.toUnscaledLong());
    } else {
      byte[] bytes = value.toJavaBigDecimal().unscaledValue().toByteArray();
      toColumn.arrayData().reserve(bytes.length);
      toColumn.arrayData().putBytes(0, bytes.length, bytes, 0);
      for (int index = 0; index < size; index++) {
        toColumn.putArray(index, 0, bytes.length);
      }
    }
  }
}
