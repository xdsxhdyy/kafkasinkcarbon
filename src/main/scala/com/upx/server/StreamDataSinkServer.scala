package com.upx.server

import java.util
import java.util.Map
import java.util.concurrent.{Callable, ConcurrentHashMap, Executors, Future}

import com.upx.base.DataBase
import com.upx.common.CommonConfig
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.parser.CarbonStreamParser
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType

/**
  * @Author luoyu
  * @create 2019/11/1 17:52
  */
object StreamDataSinkServer extends Logging{
  private val projectName = this.getClass.getName
  private val threadPoll = Executors.newFixedThreadPool(2)

  def main(args: Array[String]): Unit = {
    val carbon = SparkSession.builder().appName(projectName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.streaming.metricsEnabled", "true")
      .config("spark.streaming.backPressure.enabled", "true")
      .config("spark.streaming.kafka.maxRatePerPartition", "5000")
      .getOrCreateCarbonSession(CommonConfig.CARBON_STORE, CommonConfig.CARBON_METASTORE)

    carbon.sparkContext.setLogLevel("WARN")
    val tableContainer: ConcurrentHashMap[String, StructType] = DataBase.data.initDataContainer()
    val topicContainer: ConcurrentHashMap[String, String] = DataBase.data.initTopicContainer()
    //把kafka里面的数据拿到，再存到carbon里面

    val futures = new util.ArrayList[TaskSchedule]()
    val entrySet: util.Set[Map.Entry[String, StructType]] = tableContainer.entrySet()
    val unit: util.Iterator[Map.Entry[String, StructType]] = entrySet.iterator()

    while (unit.hasNext){
      val entyNext: Map.Entry[String, StructType] = unit.next()
      val futrue = new TaskSchedule(carbon, entyNext, topicContainer)
      futures.add(futrue)
    }

    val reList: util.List[Future[String]] = threadPoll.invokeAll(futures)
    val it = reList.iterator()
    while (it.hasNext){
      val isEnd = it.next().get()
      logWarning("-----end----"+isEnd)
      threadPoll.shutdown()
    }
    carbon.close()
  }


  class TaskSchedule(carbon: SparkSession, entyNext: Map.Entry[String, StructType]
                    ,topicContainer: ConcurrentHashMap[String, String] ) extends Callable[String]{

    override def call(): String = {
      val key = entyNext.getKey
      val value = entyNext.getValue
      val carbonTable = CarbonEnv.getCarbonTable(Some(CommonConfig.CB_DATABASE_NAME), key)(carbon)
      val tablePath = carbonTable.getTablePath
      val topic = topicContainer.get(key)

      import carbon.implicits._
      val df = carbon.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", CommonConfig.BOOTSTRAP_SERVERS)
        .option("enable.auto.commit", "true")
        .option("group.id", projectName)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
//        .option("endingOffsets", "latest")
        .option("minPartitions", "3")
        .option("failOnDataLoss", "false")
        .option("maxOffsetPerTrigger", "1000")
        .option("record_format", "json")
        .option("allowBackslashEscapingAnyCharacter", "true") //表示是否支持json中含有反斜杠，且将反斜杠忽略掉
        .load()
        .selectExpr("cast(value as string) as json")
        .select(from_json($"json", schema = value).as("data"))
        .selectExpr("data.*")

      val wdf = df.repartition(8).writeStream
        .format("carbondata")
        .trigger(Trigger.ProcessingTime("15 seconds"))
        .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(tablePath))
        .option("dbName", CommonConfig.CB_DATABASE_NAME)
        .option("numPartitions", 3)
        .option("tableName", key)
        .option(CarbonStreamParser.CARBON_STREAM_PARSER, CarbonStreamParser.CARBON_STREAM_PARSER_ROW_PARSER)
        .start()
      wdf.awaitTermination()
      key.concat("-").concat(topic)
    }
  }

}
