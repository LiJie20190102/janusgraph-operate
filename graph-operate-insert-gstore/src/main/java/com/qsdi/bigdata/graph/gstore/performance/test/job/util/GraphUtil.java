/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package com.qsdi.bigdata.graph.gstore.performance.test.job.util;

import com.alibaba.fastjson.JSONObject;
import com.qsdi.bigdata.graph.gstore.performance.test.job.entity.DataType;

import java.util.Objects;

/** @ClassName GraphUtil @Desc GraphUtil @Author LouTao123 @Date 2022/6/21 15:19 @Version 1.0 */
public class GraphUtil {
  private GraphUtil() {}

  public static Object graphValue(String columnDataType, JSONObject fieldValue) {
    String filedValue = "fieldValue";
    if (Objects.equals(columnDataType, DataType.BOOLEAN.getName())) {
      return fieldValue.getBoolean(filedValue);
    } else if (Objects.equals(columnDataType, DataType.BYTE.getName())) {
      return fieldValue.getByteValue(filedValue);
    } else if (Objects.equals(columnDataType, DataType.INT.getName())) {
      return fieldValue.getInteger(filedValue);
    } else if (Objects.equals(columnDataType, DataType.LONG.getName())) {
      return fieldValue.getLong(filedValue);
    } else if (Objects.equals(columnDataType, DataType.FLOAT.getName())) {
      return fieldValue.getFloat(filedValue);
    } else if (Objects.equals(columnDataType, DataType.DOUBLE.getName())) {
      return fieldValue.getDouble(filedValue);
    } else if (Objects.equals(columnDataType, DataType.TEXT.getName())) {
      return fieldValue.getString(filedValue);
    } else if (Objects.equals(columnDataType, DataType.BLOB.getName())) {
      return fieldValue.getString(filedValue);
    } else if (Objects.equals(columnDataType, DataType.DATE.getName())) {
      return fieldValue.getDate(filedValue);
    } else if (Objects.equals(columnDataType, DataType.UUID.getName())) {
      return fieldValue.getString(filedValue);
    } else {
      return fieldValue.getString(filedValue);
    }
  }
}
