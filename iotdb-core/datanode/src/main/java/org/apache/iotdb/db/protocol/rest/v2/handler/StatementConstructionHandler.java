/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.v2.handler;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertRecordRequest;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertTabletRequest;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

public class StatementConstructionHandler {
  private StatementConstructionHandler() {}

  public static InsertTabletStatement constructInsertTabletStatement(
      InsertTabletRequest insertTabletRequest)
      throws IllegalPathException, WriteProcessRejectException {
    // construct insert statement
    InsertTabletStatement insertStatement = new InsertTabletStatement();
    insertStatement.setDevicePath(
        DataNodeDevicePathCache.getInstance().getPartialPath(insertTabletRequest.getDevice()));
    insertStatement.setMeasurements(insertTabletRequest.getMeasurements().toArray(new String[0]));
    List<List<Object>> rawData = insertTabletRequest.getValues();
    List<String> rawDataType = insertTabletRequest.getDataTypes();

    int rowSize = insertTabletRequest.getTimestamps().size();
    int columnSize = rawDataType.size();

    Object[] columns = new Object[columnSize];
    BitMap[] bitMaps = new BitMap[columnSize];
    TSDataType[] dataTypes = new TSDataType[columnSize];

    for (int i = 0; i < columnSize; i++) {
      dataTypes[i] = TSDataType.valueOf(rawDataType.get(i).toUpperCase(Locale.ROOT));
    }

    for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
      bitMaps[columnIndex] = new BitMap(rowSize);
      switch (dataTypes[columnIndex]) {
        case BOOLEAN:
          boolean[] booleanValues = new boolean[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object data = rawData.get(columnIndex).get(rowIndex);
            if (data == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              if ("1".equals(data.toString())) {
                booleanValues[rowIndex] = true;
              } else if ("0".equals(data.toString())) {
                booleanValues[rowIndex] = false;
              } else {
                booleanValues[rowIndex] = (Boolean) data;
              }
            }
          }
          columns[columnIndex] = booleanValues;
          break;
        case INT32:
          int[] intValues = new int[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object object = rawData.get(columnIndex).get(rowIndex);
            if (object == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else if (object instanceof Integer) {
              intValues[rowIndex] = (int) object;
            } else {
              throw new WriteProcessRejectException(
                  "unsupported data type: " + object.getClass().toString());
            }
          }
          columns[columnIndex] = intValues;
          break;
        case INT64:
          long[] longValues = new long[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object object = rawData.get(columnIndex).get(rowIndex);
            if (object == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else if (object instanceof Integer) {
              longValues[rowIndex] = (int) object;
            } else if (object instanceof Long) {
              longValues[rowIndex] = (long) object;
            } else {
              throw new WriteProcessRejectException(
                  "unsupported data type: " + object.getClass().toString());
            }
          }
          columns[columnIndex] = longValues;
          break;
        case FLOAT:
          float[] floatValues = new float[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object data = rawData.get(columnIndex).get(rowIndex);
            if (data == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              floatValues[rowIndex] = Float.valueOf(String.valueOf(data));
            }
          }
          columns[columnIndex] = floatValues;
          break;
        case DOUBLE:
          double[] doubleValues = new double[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            if (rawData.get(columnIndex).get(rowIndex) == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              doubleValues[rowIndex] =
                  Double.valueOf(String.valueOf(rawData.get(columnIndex).get(rowIndex)));
            }
          }
          columns[columnIndex] = doubleValues;
          break;
        case TEXT:
          Binary[] binaryValues = new Binary[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            if (rawData.get(columnIndex).get(rowIndex) == null) {
              bitMaps[columnIndex].mark(rowIndex);
              binaryValues[rowIndex] = new Binary("".getBytes(StandardCharsets.UTF_8));
            } else {
              binaryValues[rowIndex] =
                  new Binary(
                      rawData
                          .get(columnIndex)
                          .get(rowIndex)
                          .toString()
                          .getBytes(StandardCharsets.UTF_8));
            }
          }
          columns[columnIndex] = binaryValues;
          break;
        default:
          throw new IllegalArgumentException("Invalid input: " + rawDataType.get(columnIndex));
      }
    }

    insertStatement.setTimes(
        insertTabletRequest.getTimestamps().stream().mapToLong(Long::longValue).toArray());
    insertStatement.setColumns(columns);
    insertStatement.setBitMaps(bitMaps);
    insertStatement.setRowCount(insertTabletRequest.getTimestamps().size());
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setAligned(insertTabletRequest.getIsAligned());
    return insertStatement;
  }

  public static InsertRowStatement constructInsertRowStatement(
      InsertRecordRequest insertRecordRequest)
      throws IllegalPathException, WriteProcessRejectException {
    // construct insert statement
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    insertRowStatement.setDevicePath(
        DataNodeDevicePathCache.getInstance().getPartialPath(insertRecordRequest.getDevice()));
    insertRowStatement.setMeasurements(
        insertRecordRequest.getMeasurements().toArray(new String[0]));
    List<Object> rawData = insertRecordRequest.getValues();
    List<String> rawDataType = insertRecordRequest.getDataTypes();
    int columnSize = rawDataType.size();
    Object[] columns = new Object[columnSize];
    TSDataType[] dataTypes = new TSDataType[columnSize];

    for (int i = 0; i < columnSize; i++) {
      dataTypes[i] = TSDataType.valueOf(rawDataType.get(i).toUpperCase(Locale.ROOT));
    }

    for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
      switch (dataTypes[columnIndex]) {
        case BOOLEAN:
          Boolean booleanValue = null;
          Object dataB = rawData.get(columnIndex);
          if (dataB != null) {
            if ("1".equals(dataB.toString())) {
              booleanValue = true;
            } else if ("0".equals(dataB.toString())) {
              booleanValue = false;
            } else {
              booleanValue = (Boolean) dataB;
            }
          }
          columns[columnIndex] = booleanValue;
          break;
        case INT32:
          Integer intValue;
          Object dataI = rawData.get(columnIndex);
          if (dataI == null) {
            intValue = null;
          } else if (dataI instanceof Integer) {
            intValue = (int) dataI;
          } else {
            throw new WriteProcessRejectException(
                "unsupported data type: " + dataI.getClass().toString());
          }
          columns[columnIndex] = intValue;
          break;
        case INT64:
          Long longValue;
          Object dataL = rawData.get(columnIndex);
          if (dataL == null) {
            longValue = null;
          } else if (dataL instanceof Integer) {
            longValue = Long.valueOf(dataL.toString());
          } else if (dataL instanceof Long) {
            longValue = (long) dataL;
          } else {
            throw new WriteProcessRejectException(
                "unsupported data type: " + dataL.getClass().toString());
          }
          columns[columnIndex] = longValue;
          break;
        case FLOAT:
          Float floatValue = null;
          Object dataF = rawData.get(columnIndex);
          if (dataF != null) {
            floatValue = Float.valueOf(String.valueOf(dataF));
          }
          columns[columnIndex] = floatValue;
          break;
        case DOUBLE:
          Double doubleValue = null;
          Object dataD = rawData.get(columnIndex);
          if (dataD != null) {
            doubleValue = Double.valueOf(String.valueOf(dataD));
          }
          columns[columnIndex] = doubleValue;
          break;
        case TEXT:
          Binary binaryValue;
          Object dataT = rawData.get(columnIndex);
          if (dataT == null) {
            binaryValue = new Binary("".getBytes(StandardCharsets.UTF_8));
          } else {
            binaryValue = new Binary(dataT.toString().getBytes(StandardCharsets.UTF_8));
          }
          columns[columnIndex] = binaryValue;
          break;
        default:
          throw new IllegalArgumentException("Invalid input: " + rawDataType.get(columnIndex));
      }
    }

    insertRowStatement.setTime(Long.parseLong(insertRecordRequest.getTimestamp()));
    insertRowStatement.setValues(columns);
    insertRowStatement.setDataTypes(dataTypes);
//    insertRowStatement.setNeedInferType(true);
    insertRowStatement.setAligned(insertRecordRequest.getIsAligned());
    return insertRowStatement;
  }
}
