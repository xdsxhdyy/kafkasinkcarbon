package com.upx.base

import java.util.concurrent.ConcurrentHashMap

import com.upx.common.CommonConfig
import com.upx.model.SchemaModel
import org.apache.spark.sql.types.StructType

/**
  * @Author luoyu
  * @create 2019/11/4 11:28
  */
class DataBase {

  def initDataContainer(): ConcurrentHashMap[String, StructType] = {
    val dataContainer = new ConcurrentHashMap[String, StructType]()
    val schemaModel = new SchemaModel
    dataContainer.put(CommonConfig.CB_Data_TableClass.toLowerCase, schemaModel.tabletclass)
    dataContainer.put(CommonConfig.CB_Data_TableRelation.toLowerCase, schemaModel.tablerelation)
    dataContainer
  }

  def initTopicContainer(): ConcurrentHashMap[String, String] = {
    val topicContainer = new ConcurrentHashMap[String, String]()
    topicContainer.put(CommonConfig.CB_Data_TableClass.toLowerCase, CommonConfig.CB_Data_TableClass)
    topicContainer.put(CommonConfig.CB_Data_TableRelation.toLowerCase, CommonConfig.CB_Data_TableRelation)
    topicContainer
  }

}


object DataBase {
  val data = new DataBase
}