package com.upx.model

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

/**
  * @Author luoyu
  * @create 2019/11/4 11:29
  */
class SchemaModel {
  val tabletclass = StructType(
    Array(
      StructField("Id", IntegerType),
      StructField("Name", StringType),
      StructField("OrderId", IntegerType),
      StructField("Price", DoubleType),
      StructField("IsEveryday", IntegerType),
      StructField("SaleStartDate", StringType),
      StructField("SaleEndDate", StringType),
      StructField("SalePrice", DoubleType),
      StructField("Remark", StringType),
      StructField("IsDeleted", IntegerType),
      StructField("DeleterUserId", LongType),
      StructField("DeletionTime", StringType),
      StructField("LastModificationTime", StringType),
      StructField("LastModifierUserId", LongType),
      StructField("CreationTime", StringType),
      StructField("CreatorUserId", LongType)
    ))

  val tablerelation = StructType(
    Array(
      StructField("Id", LongType),
      StructField("BindStatus", IntegerType),
      StructField("IsDeleted", IntegerType),
      StructField("DeleterUserId", LongType),
      StructField("DeletionTime", StringType),
      StructField("LastModificationTime", StringType),
      StructField("LastModifierUserId", LongType),
      StructField("CreationTime", StringType),
      StructField("CreatorUserId", LongType)
    ))


}
