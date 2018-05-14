/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 10/28/17 9:54 AM.
 * Author: wuyufei.
 */

package com.atguigu.page

import java.util.UUID

import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.model.UserVisitAction
import com.atguigu.commons.utils.{DateUtils, NumberUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * 页面单跳转化率模块spark作业
  */
object PageOneStepConvertRate {

  def main(args: Array[String]): Unit = {

    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    // 构建Spark上下文
    val sparkConf = new SparkConf().setAppName("SessionAnalyzer").setMaster("local[*]")

    // 创建Spark客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    // 查询指定日期范围内的用户访问行为数据
    val actionRDD = this.getActionRDDByDateRange(spark, taskParam)

    // 将用户行为信息转换为 K-V 结构
    val sessionid2actionRDD = actionRDD.map(item => (item.session_id, item))

    // 将数据进行内存缓存
    sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY)

    // 对<sessionid,访问行为> RDD，做一次groupByKey操作，生成页面切片
    val sessionid2actionsRDD = sessionid2actionRDD.groupByKey()

    // 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
    val pageSplitRDD = generateAndMatchPageSplit(sc, sessionid2actionsRDD, taskParam)
    val pageSplitPvMap = pageSplitRDD.countByKey

    // 使用者指定的页面流是3,2,5,8,6
    // 咱们现在拿到的这个pageSplitPvMap，3->2，2->5，5->8，8->6
    val startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD)

    // 计算目标页面流的各个页面切片的转化率
    val convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv)

    // 持久化页面切片转化率
    persistConvertRate(spark, taskUUID, convertRateMap)

    spark.close()

  }

  /**
    * 持久化转化率
    * @param convertRateMap
    */
  def persistConvertRate(spark:SparkSession, taskid:String, convertRateMap:collection.Map[String, Double]) {

    val convertRate = convertRateMap.map(item => item._1 + "=" + item._2).mkString("|")

    val pageSplitConvertRateRDD = spark.sparkContext.makeRDD(Array(PageSplitConvertRate(taskid,convertRate)))

    import spark.implicits._
    pageSplitConvertRateRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 计算页面切片转化率
    * @param pageSplitPvMap 页面切片pv
    * @param startPagePv 起始页面pv
    * @return
    */
  def computePageSplitConvertRate(taskParam:JSONObject, pageSplitPvMap:collection.Map[String, Long], startPagePv:Long):collection.Map[String, Double] = {

    val convertRateMap = new mutable.HashMap[String, Double]()
    //1,2,3,4,5,6,7
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val targetPages = targetPageFlow.split(",").toList
    //(1_2,2_3,3_4,4_5,5_6,6_7)
    val targetPagePairs = targetPages.slice(0, targetPages.length-1).zip(targetPages.tail).map(item => item._1 + "_" + item._2)

    var lastPageSplitPv = startPagePv.toDouble
    // 3,5,2,4,6
    // 3_5
    // 3_5 pv / 3 pv
    // 5_2 rate = 5_2 pv / 3_5 pv

    // 通过for循环，获取目标页面流中的各个页面切片（pv）

    for(targetPage <- targetPagePairs){
      val targetPageSplitPv = pageSplitPvMap.get(targetPage).get.toDouble
      println((targetPageSplitPv, lastPageSplitPv))
      val convertRate = NumberUtils.formatDouble(targetPageSplitPv / lastPageSplitPv, 2)
      convertRateMap.put(targetPage, convertRate)
      lastPageSplitPv = targetPageSplitPv
    }

    convertRateMap
  }

  /**
    * 获取页面流中初始页面的pv
    * @param taskParam
    * @param sessionid2actionsRDD
    * @return
    */
  def getStartPagePv(taskParam:JSONObject, sessionid2actionsRDD:RDD[(String, Iterable[UserVisitAction])]) :Long = {

    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    //获取起始页面ID
    val startPageId = targetPageFlow.split(",")(0).toLong

    val startPageRDD = sessionid2actionsRDD.flatMap{ case (sessionid, userVisitActions) =>
      userVisitActions.filter(_.page_id == startPageId).map(_.page_id)
    }

    startPageRDD.count()
  }

  /**
    * 页面切片生成与匹配算法
    * @param sc
    * @param sessionid2actionsRDD
    * @param taskParam
    * @return
    */
  def generateAndMatchPageSplit(sc:SparkContext, sessionid2actionsRDD:RDD[(String, Iterable[UserVisitAction])], taskParam:JSONObject ):RDD[(String, Int)] = {
    //1,2,3,4,5,6,7
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val targetPages = targetPageFlow.split(",").toList
    //(1_2,2_3,3_4,4_5,5_6,6_7)
    val targetPagePairs = targetPages.slice(0, targetPages.length-1).zip(targetPages.tail).map(item => item._1 + "_" + item._2)
    val targetPageFlowBroadcast = sc.broadcast(targetPagePairs)

    sessionid2actionsRDD.flatMap{ case (sessionid, userVisitActions) =>
      // 获取使用者指定的页面流
      // 使用者指定的页面流，1,2,3,4,5,6,7
      // 1->2的转化率是多少？2->3的转化率是多少？

      // 这里，我们拿到的session的访问行为，默认情况下是乱序的
      // 比如说，正常情况下，我们希望拿到的数据，是按照时间顺序排序的
      // 但是问题是，默认是不排序的
      // 所以，我们第一件事情，对session的访问行为数据按照时间进行排序

      // 举例，反例
      // 比如，3->5->4->10->7
      // 3->4->5->7->10
      // 排序
      val sortedUVAs = userVisitActions.toList.sortWith((uva1, uva2) => DateUtils.parseTime(uva1.action_time).getTime() < DateUtils.parseTime(uva2.action_time).getTime())
      val soredPages = sortedUVAs.map(item => if(item.page_id != null) item.page_id)
      //生成所有页面切片
      val sessionPagePairs = soredPages.slice(0, soredPages.length-1).zip(soredPages.tail).map(item => item._1 + "_" + item._2)
      //过滤掉不符合的页面切片，并转换成元组返回
      sessionPagePairs.filter(targetPageFlowBroadcast.value.contains(_)).map((_,1))
    }
  }


  def getActionRDDByDateRange(spark:SparkSession, taskParam:JSONObject): RDD[UserVisitAction] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    import spark.implicits._
    spark.sql("select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'")
      .as[UserVisitAction].rdd
  }

}
