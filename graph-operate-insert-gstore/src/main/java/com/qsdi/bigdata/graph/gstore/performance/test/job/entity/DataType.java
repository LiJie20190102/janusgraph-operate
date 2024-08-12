package com.qsdi.bigdata.graph.gstore.performance.test.job.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

public enum DataType {
  OBJECT(1, "object", Serializable.class),
  BOOLEAN(2, "boolean", Boolean.class),
  BYTE(3, "byte", Byte.class),
  INT(4, "int", Integer.class),
  LONG(5, "long", Long.class),
  FLOAT(6, "float", Float.class),
  DOUBLE(7, "double", Double.class),
  TEXT(8, "text", String.class),
  BLOB(9, "blob", byte[].class),
  DATE(10, "date", Date.class),
  UUID(11, "uuid", UUID.class);

  private byte code = 0;
  private String name = null;
  private Class<?> clazz = null;

  DataType(int code, String name, Class<?> clazz) {
    assert code < 256;
    this.code = (byte) code;
    this.name = name;
    this.clazz = clazz;
  }

  public byte code() {
    return this.code;
  }

  public String string() {
    return this.name;
  }

  public Class<?> clazz() {
    return this.clazz;
  }

  public boolean isNumber() {
    return this == BYTE || this == INT || this == LONG || this == FLOAT || this == DOUBLE;
  }

  public boolean isDate() {
    return this == DataType.DATE;
  }

  public boolean isUUID() {
    return this == DataType.UUID;
  }

  public boolean isBoolean() {
    return this == BOOLEAN;
  }

  public String getName() {
    return name;
  }
}
