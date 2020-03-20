package com.upx.common

import com.typesafe.config.ConfigFactory

/**
  * @Author luoyu
  * @create 2019/11/4 9:56
  */
object CommonConfig {
  private val config = ConfigFactory.load()

  val CARBON_STORE = config.getString("store_path")
  val CARBON_METASTORE = config.getString("metastore_path")
  val CB_DATABASE_NAME = config.getString("database_name")
  val BOOTSTRAP_SERVERS = config.getString("bootstrap_servers")

  val CB_Data_TableClass = "TableClass"
  val CB_Data_TableRelation = "TableRelation"

}
