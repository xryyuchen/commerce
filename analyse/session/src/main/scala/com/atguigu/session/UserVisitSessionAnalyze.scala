/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 10/26/17 9:59 PM.
 * Author: wuyufei.
 */

package com.atguigu.session

import java.util.{Date, UUID}

import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.model._
import com.atguigu.commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/**
  * 用户访问session统计分析
  *
  * 接收用户创建的分析任务，用户可能指定的条件如下：
  *
  * 1、时间范围：起始日期~结束日期
  * 2、性别：男或女
  * 3、年龄范围
  * 4、职业：多选
  * 5、城市：多选
  * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
  * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
  *
  * @author wuyufei
  *
  */
object UserVisitSessionAnalyze {

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

    // 首先要从user_visit_action的Hive表中，查询出来指定日期范围内的行为数据
    val actionRDD = this.getActionRDDByDateRange(spark, taskParam)

    // 将用户行为信息转换为 K-V 结构
    val sessionid2actionRDD = actionRDD.map(item => (item.session_id, item))

    // 将数据进行内存缓存
    sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY)

    // 将数据转换为Session粒度， 格式为<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
    val sessionid2AggrInfoRDD = this.aggregateBySession(spark, sessionid2actionRDD)

    // 设置自定义累加器，实现所有数据的统计功能,注意累加器也是懒执行的
    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator

    // 注册自定义累加器
    sc.register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator")

    // 根据查询任务的配置，过滤用户的行为数据，同时在过滤的过程中，对累加器中的数据进行统计
    val filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator)

    // 对数据进行内存缓存
    filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY)

//    println("filteredSessionid2AggrInfoRDD = "+filteredSessionid2AggrInfoRDD.count())



    // sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
    val sessionid2detailRDD = getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2actionRDD)

    // 对数据进行内存缓存
    sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY)

    // 业务功能二：随机均匀获取Session，之所以业务功能二先计算，是为了通过Action操作触发所有转换操作。
//    randomExtractSession(spark, taskUUID, filteredSessionid2AggrInfoRDD, sessionid2detailRDD)

    // 业务功能一：统计各个范围的session占比，并写入MySQL
//    calculateAndPersistAggrStat(spark, sessionAggrStatAccumulator.value, taskUUID)

    // 业务功能三：获取top10热门品类
    val top10CategoryList = getTop10Category(spark, taskUUID, sessionid2detailRDD)

    // 业务功能四：获取top10活跃session
    getTop10Session(spark, taskUUID, top10CategoryList, sessionid2detailRDD)

    // 关闭Spark上下文
    spark.close()
  }


  /**
    * 业务功能四：获取top10活跃session
    *
    * @param taskid
    */
  def getTop10Session(spark: SparkSession, taskid: String, top10CategoryList: Array[(CategorySortKey, String)], sessionid2ActionRDD: RDD[(String, UserVisitAction)]) {

    // 第一步：将top10热门品类的id，生成一份RDD
    val top10CategoryIdRDD = spark.sparkContext.makeRDD(top10CategoryList.map { case (categorySortKey, line) =>
      val categoryid = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CATEGORY_ID).toLong;
      (categoryid, categoryid)
    })

    // 第二步：计算top10品类被各session点击的次数
    val sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey()

    // 获取每个品类被每一个Session的点击
    val categoryid2sessionCountRDD = sessionid2ActionsRDD.flatMap { case (sessionid, userVisitActions) =>
      val categoryCountMap = new mutable.HashMap[Long, Long]()
      for (userVisitAction <- userVisitActions) {
        if (!categoryCountMap.contains(userVisitAction.click_category_id))
          categoryCountMap.put(userVisitAction.click_category_id, 0)

        if (userVisitAction.click_category_id != null && userVisitAction.click_category_id != -1L) {
          categoryCountMap.update(userVisitAction.click_category_id, categoryCountMap(userVisitAction.click_category_id) + 1)
        }
      }

      for ((categoryid, count) <- categoryCountMap)
        yield (categoryid, sessionid + "," + count)

    }

    // 获取到to10热门品类，被各个session点击的次数【将数据集缩小】
    val top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD).map { case (cid, (ccid, value)) => (cid, value) }

    // 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
    val top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey()

    // 将每一个品类的所有点击排序，取前十个，并转换为对象
    val top10SessionObjectRDD = top10CategorySessionCountsRDD.flatMap { case (categoryid, clicks) =>
      val top10Sessions = clicks.toList.sortWith(_.split(",")(1) > _.split(",")(1)).take(10)
      top10Sessions.map { case line =>
        val sessionid = line.split(",")(0)
        val count = line.split(",")(1).toLong
        Top10Session(taskid, categoryid, sessionid, count)
      }
    }

    // 将结果以追加方式写入到MySQL中
    import spark.implicits._
    top10SessionObjectRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_session")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    val top10SessionRDD = top10SessionObjectRDD.map(item => (item.sessionid, item.sessionid))

    // 第四步：获取top10活跃session的明细数据
    val sessionDetailRDD = top10SessionRDD.join(sessionid2ActionRDD).map { case (sid, (sessionid, userVisitAction)) =>
      SessionDetail(taskid, userVisitAction.user_id, userVisitAction.session_id,
        userVisitAction.page_id, userVisitAction.action_time, userVisitAction.search_keyword,
        userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
        userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
    }

    // 将活跃Session的明细数据，写入到MySQL
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_detail")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }

  /**
    * 获取各品类点击次数RDD
    *
    * @param sessionid2detailRDD
    * @return
    */
  def getClickCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    // 只将点击行为过滤出来
    val clickActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.click_category_id != null }

    // 获取每种类别的点击次数
    val clickCategoryIdRDD = clickActionRDD.map { case (sessionid, userVisitAction) => (userVisitAction.click_category_id, 1L) }

    // 计算各个品类的点击次数
    clickCategoryIdRDD.reduceByKey(_ + _)
  }

  /**
    * 获取各品类的下单次数RDD
    *
    * @param sessionid2detailRDD
    * @return
    */
  def getOrderCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    // 过滤订单数据
    val orderActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.order_category_ids != null }

    // 获取每种类别的下单次数
    val orderCategoryIdRDD = orderActionRDD.flatMap { case (sessionid, userVisitAction) => userVisitAction.order_category_ids.split(",").map(item => (item.toLong, 1L)) }

    // 计算各个品类的下单次数
    orderCategoryIdRDD.reduceByKey(_ + _)
  }

  /**
    * 获取各个品类的支付次数RDD
    *
    * @param sessionid2detailRDD
    * @return
    */
  def getPayCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    // 过滤支付数据
    val payActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.pay_category_ids != null }

    // 获取每种类别的支付次数
    val payCategoryIdRDD = payActionRDD.flatMap { case (sessionid, userVisitAction) => userVisitAction.pay_category_ids.split(",").map(item => (item.toLong, 1L)) }

    // 计算各个品类的支付次数
    payCategoryIdRDD.reduceByKey(_ + _)
  }

  /**
    * 连接品类RDD与数据RDD
    *
    * @param categoryidRDD
    * @param clickCategoryId2CountRDD
    * @param orderCategoryId2CountRDD
    * @param payCategoryId2CountRDD
    * @return
    */
  def joinCategoryAndData(categoryidRDD: RDD[(Long, Long)], clickCategoryId2CountRDD: RDD[(Long, Long)], orderCategoryId2CountRDD: RDD[(Long, Long)], payCategoryId2CountRDD: RDD[(Long, Long)]): RDD[(Long, String)] = {

    // 将所有品类信息与点击次数信息结合【左连接】
    val clickJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD).map { case (categoryid, (cid, optionValue)) =>
      val clickCount = if (optionValue.isDefined) optionValue.get else 0L
      val value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount
      (categoryid, value)
    }

    // 将所有品类信息与订单次数信息结合【左连接】
    val orderJoinRDD = clickJoinRDD.leftOuterJoin(orderCategoryId2CountRDD).map { case (categoryid, (ovalue, optionValue)) =>
      val orderCount = if (optionValue.isDefined) optionValue.get else 0L
      val value = ovalue + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
      (categoryid, value)
    }

    // 将所有品类信息与付款次数信息结合【左连接】
    val payJoinRDD = orderJoinRDD.leftOuterJoin(payCategoryId2CountRDD).map { case (categoryid, (ovalue, optionValue)) =>
      val payCount = if (optionValue.isDefined) optionValue.get else 0L
      val value = ovalue + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
      (categoryid, value)
    }

    payJoinRDD
  }

  /**
    * 业务需求三：获取top10热门品类
    *
    * @param spark
    * @param taskid
    * @param sessionid2detailRDD
    * @return
    */
  def getTop10Category(spark: SparkSession, taskid: String, sessionid2detailRDD: RDD[(String, UserVisitAction)]): Array[(CategorySortKey, String)] = {

    // 第一步：获取每一个Sessionid 点击过、下单过、支付过的数量
    val categoryidRDD = sessionid2detailRDD.flatMap { case (sessionid, userVisitAction) =>
      val list = ArrayBuffer[(Long, Long)]()

      if (userVisitAction.click_category_id != null) {
        list += ((userVisitAction.click_category_id, userVisitAction.click_category_id))
      }
      if (userVisitAction.order_category_ids != null) {
        for (orderCategoryId <- userVisitAction.order_category_ids.split(","))
          list += ((orderCategoryId.toLong, orderCategoryId.toLong))
      }
      if (userVisitAction.pay_category_ids != null) {
        for (payCategoryId <- userVisitAction.pay_category_ids.split(","))
          list += ((payCategoryId.toLong, payCategoryId.toLong))
      }
      list
    }

    // 对重复的categoryid进行去重
    val distinctCategoryIdRDD = categoryidRDD.distinct

    // 第二步：计算各品类的点击、下单和支付的次数

    // 计算各个品类的点击次数
    val clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD)
    // 计算各个品类的下单次数
    val orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD)
    // 计算各个品类的支付次数
    val payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD)

    // 第三步：join各品类与它的点击、下单和支付的次数
    val categoryid2countRDD = joinCategoryAndData(distinctCategoryIdRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);

    // 第四步：自定义二次排序key

    // 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
    val sortKey2countRDD = categoryid2countRDD.map { case (categoryid, line) =>
      val clickCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT).toLong
      (CategorySortKey(clickCount, orderCount, payCount), line)
    }

    // 降序排序
    val sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false)

    // 第六步：用take(10)取出top10热门品类，并写入MySQL
    val top10CategoryList = sortedCategoryCountRDD.take(10)
    val top10Category = top10CategoryList.map { case (categorySortKey, line) =>
      val categoryid = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      val clickCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT).toLong

      Top10Category(taskid, categoryid, clickCount, orderCount, payCount)
    }

    val top10CategoryRDD = spark.sparkContext.makeRDD(top10Category)

    import spark.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_category")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    top10CategoryList
  }

  /**
    * 业务需求二：随机抽取session
    *
    * @param sessionid2AggrInfoRDD
    */
  def randomExtractSession(spark: SparkSession, taskUUID: String, sessionid2AggrInfoRDD: RDD[(String, String)], sessionid2detailRDD: RDD[(String, UserVisitAction)]) {

    // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
    val time2sessionidRDD = sessionid2AggrInfoRDD.map { case (sessionid, aggrInfo) =>
      val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
      val dateHour = DateUtils.getDateHour(startTime)
      (dateHour, aggrInfo)
    }

    // 得到每天每小时的session数量
    val countMap = time2sessionidRDD.countByKey()

    // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引，将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    val dateHourCountMap = mutable.HashMap[String, mutable.HashMap[String, Long]]()
    for ((dateHour, count) <- countMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long](); dateHourCountMap(date) += (hour -> count)
        case Some(hourCountMap) => hourCountMap += (hour -> count)
      }
    }

    // 按时间比例随机抽取算法，总共要抽取100个session，先按照天数，进行平分
    val extractNumberPerDay = 100 / dateHourCountMap.size

    val dateHourExtractMap = mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[Int]]]()
    val random = new Random()

    // 遍历每个小时，填充Map<date,<hour,(3,5,20,102)>>
    def hourExtractMapFunc(hourExtractMap: mutable.HashMap[String, mutable.ListBuffer[Int]], hourCountMap: mutable.HashMap[String, Long], sessionCount: Long) {

      for ((hour, count) <- hourCountMap) {
        // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
        // 就可以计算出，当前小时需要抽取的session数量
        var hourExtractNumber = ((count / sessionCount) * extractNumberPerDay).toInt
        if (hourExtractNumber > count) {
          hourExtractNumber = count.toInt
        }

        hourExtractMap.get(hour) match {
          case None => hourExtractMap(hour) = new mutable.ListBuffer[Int]();
            for (i <- 0 to hourExtractNumber) {
              var extractIndex = random.nextInt(count.toInt);
              while (hourExtractMap(hour).contains(extractIndex)) {
                extractIndex = random.nextInt(count.toInt);
              }
              hourExtractMap(hour) += (extractIndex)
            }
          case Some(extractIndexList) =>
            for (i <- 0 to hourExtractNumber) {
              var extractIndex = random.nextInt(count.toInt);
              while (hourExtractMap(hour).contains(extractIndex)) {
                extractIndex = random.nextInt(count.toInt);
              }
              hourExtractMap(hour) += (extractIndex)
            }
        }
      }
    }

    // session随机抽取功能
    for ((date, hourCountMap) <- dateHourCountMap) {

      // 计算出这一天的session总数
      val sessionCount = hourCountMap.values.sum

      dateHourExtractMap.get(date) match {
        case None => dateHourExtractMap(date) = new mutable.HashMap[String, mutable.ListBuffer[Int]]();
          hourExtractMapFunc(dateHourExtractMap(date), hourCountMap, sessionCount)
        case Some(hourExtractMap) => hourExtractMapFunc(hourExtractMap, hourCountMap, sessionCount)
      }
    }

    //将Map进行广播
    val dateHourExtractMapBroadcast = spark.sparkContext.broadcast(dateHourExtractMap)


    // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
    val time2sessionsRDD = time2sessionidRDD.groupByKey()

    // 第三步：遍历每天每小时的session，然后根据随机索引进行抽取,我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
    val sessionRandomExtract = time2sessionsRDD.flatMap { case (dateHour, items) =>
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      val dateHourExtractMap = dateHourExtractMapBroadcast.value
      val extractIndexList = dateHourExtractMap.get(date).get(hour)

      var index = 0
      val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()
      for (sessionAggrInfo <- items) {

        if (extractIndexList.contains(index)) {
          val sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
          val starttime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME)
          val searchKeywords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
          val clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
          sessionRandomExtractArray += SessionRandomExtract(taskUUID, sessionid, starttime, searchKeywords, clickCategoryIds)
        }

        index += 1
      }
      sessionRandomExtractArray
    }

    // 将抽取后的数据保存到MySQL
    import spark.implicits._
    sessionRandomExtract.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_random_extract")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    val extractSessionidsRDD = sessionRandomExtract.map(item => (item.sessionid, item.sessionid))

    // 第四步：获取抽取出来的session的明细数据
    val extractSessionDetailRDD = extractSessionidsRDD.join(sessionid2detailRDD)

    val sessionDetailRDD = extractSessionDetailRDD.map { case (sid, (sessionid, userVisitAction)) =>
      SessionDetail(taskUUID, userVisitAction.user_id, userVisitAction.session_id,
        userVisitAction.page_id, userVisitAction.action_time, userVisitAction.search_keyword,
        userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
        userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
    }

    // 将明细数据保存到MySQL中
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_detail")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 计算各session范围占比，并写入MySQL
    *
    * @param value
    */
  def calculateAndPersistAggrStat(spark: SparkSession, value: mutable.HashMap[String, Int], taskUUID: String) {
    // 从Accumulator统计串中获取值
    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = SessionAggrStat(taskUUID,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    import spark.implicits._
    val sessionAggrStatRDD = spark.sparkContext.makeRDD(Array(sessionAggrStat))
    sessionAggrStatRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_aggr_stat")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 获取通过筛选条件的session的访问明细数据RDD
    *
    * @param sessionid2aggrInfoRDD
    * @param sessionid2actionRDD
    * @return
    */
  def getSessionid2detailRDD(sessionid2aggrInfoRDD: RDD[(String, String)], sessionid2actionRDD: RDD[(String, UserVisitAction)]): RDD[(String, UserVisitAction)] = {
    sessionid2aggrInfoRDD.join(sessionid2actionRDD).map(item => (item._1, item._2._2))
  }

  /**
    * 业务需求一：过滤session数据，并进行聚合统计
    *
    * @param sessionid2AggrInfoRDD
    * @return
    */
  def filterSessionAndAggrStat(sessionid2AggrInfoRDD: RDD[(String, String)],
                               taskParam: JSONObject,
                               sessionAggrStatAccumulator: AccumulatorV2[String, mutable.HashMap[String, Int]]): RDD[(String, String)] = {

    // 获取查询任务中的配置
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var _parameter = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (_parameter.endsWith("\\|")) {
      _parameter = _parameter.substring(0, _parameter.length() - 1)
    }

    val parameter = _parameter

    // 根据筛选参数进行过滤
    val filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter { case (sessionid, aggrInfo) =>
      // 接着，依次按照筛选条件进行过滤
      // 按照年龄范围进行过滤（startAge、endAge）
      var success = true
      if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
        success = false


      // 按照职业范围进行过滤（professionals）
      // 互联网,IT,软件
      // 互联网
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS))
        success = false

      // 按照城市范围进行过滤（cities）
      // 北京,上海,广州,深圳
      // 成都
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES))
        success = false

      // 按照性别进行过滤
      // 男/女
      // 男，女
      if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX))
        success = false

      // 按照搜索词进行过滤
      // 我们的session可能搜索了 火锅,蛋糕,烧烤
      // 我们的筛选条件可能是 火锅,串串香,iphone手机
      // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
      // 任何一个搜索词相当，即通过
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS))
        success = false

      // 按照点击品类id进行过滤
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS))
        success = false

      // 如果符合任务搜索需求
      if (success) {
        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

        // 计算访问时长范围
        def calculateVisitLength(visitLength: Long) {
          if (visitLength >= 1 && visitLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
          } else if (visitLength >= 4 && visitLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
          } else if (visitLength >= 7 && visitLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
          } else if (visitLength >= 10 && visitLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
          } else if (visitLength > 30 && visitLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
          } else if (visitLength > 60 && visitLength <= 180) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
          } else if (visitLength > 180 && visitLength <= 600) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
          } else if (visitLength > 600 && visitLength <= 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
          } else if (visitLength > 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
          }
        }

        // 计算访问步长范围
        def calculateStepLength(stepLength: Long) {
          if (stepLength >= 1 && stepLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
          } else if (stepLength >= 4 && stepLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
          } else if (stepLength >= 7 && stepLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
          } else if (stepLength >= 10 && stepLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
          } else if (stepLength > 30 && stepLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
          } else if (stepLength > 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
          }
        }

        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
        val visitLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        val stepLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
        calculateVisitLength(visitLength)
        calculateStepLength(stepLength)
      }
      success
    }

    filteredSessionid2AggrInfoRDD
  }

  /**
    * 对Session数据进行聚合
    * @param spark
    * @param sessinoid2actionRDD
    * @return
    */
  def aggregateBySession(spark: SparkSession, sessinoid2actionRDD: RDD[(String, UserVisitAction)]): RDD[(String, String)] = {

    // 对行为数据按session粒度进行分组
    val sessionid2ActionsRDD = sessinoid2actionRDD.groupByKey()

    // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来，<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
    val userid2PartAggrInfoRDD = sessionid2ActionsRDD.map { case (sessionid, userVisitActions) =>

      val searchKeywordsBuffer = new StringBuffer("")
      val clickCategoryIdsBuffer = new StringBuffer("")

      var userid = -1L

      // session的起始和结束时间
      var startTime: Date = null
      var endTime: Date = null

      // session的访问步长
      var stepLength = 0

      // 遍历session所有的访问行为
      userVisitActions.foreach { userVisitAction =>
        if (userid == -1L) {
          userid = userVisitAction.user_id
        }
        val searchKeyword = userVisitAction.search_keyword
        val clickCategoryId = userVisitAction.click_category_id

        // 实际上这里要对数据说明一下
        // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
        // 其实，只有搜索行为，是有searchKeyword字段的
        // 只有点击品类的行为，是有clickCategoryId字段的
        // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

        // 我们决定是否将搜索词或点击品类id拼接到字符串中去
        // 首先要满足：不能是null值
        // 其次，之前的字符串中还没有搜索词或者点击品类id

        if (StringUtils.isNotEmpty(searchKeyword)) {
          if (!searchKeywordsBuffer.toString.contains(searchKeyword)) {
            searchKeywordsBuffer.append(searchKeyword + ",")
          }
        }
        if (clickCategoryId != null && clickCategoryId != -1L) {
          if (!clickCategoryIdsBuffer.toString.contains(clickCategoryId.toString)) {
            clickCategoryIdsBuffer.append(clickCategoryId + ",")
          }
        }

        // 计算session开始和结束时间
        val actionTime = DateUtils.parseTime(userVisitAction.action_time)

        if (startTime == null) {
          startTime = actionTime
        }
        if (endTime == null) {
          endTime = actionTime
        }

        if (actionTime.before(startTime)) {
          startTime = actionTime
        }
        if (actionTime.after(endTime)) {
          endTime = actionTime
        }

        // 计算session访问步长
        stepLength += 1
      }

      val searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString)
      val clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

      // 计算session访问时长（秒）
      val visitLength = (endTime.getTime() - startTime.getTime()) / 1000

      // 聚合数据，使用key=value|key=value
      val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

      (userid, partAggrInfo);
    }

    // 查询所有用户数据，并映射成<userid,Row>的格式
    import spark.implicits._
    val userid2InfoRDD = spark.sql("select * from user_info").as[UserInfo].rdd.map(item => (item.user_id, item))

    // 将session粒度聚合数据，与用户信息进行join
    val userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

    // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
    val sessionid2FullAggrInfoRDD = userid2FullInfoRDD.map { case (uid, (partAggrInfo, userInfo)) =>
      val sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)

      val fullAggrInfo = partAggrInfo + "|" +
        Constants.FIELD_AGE + "=" + userInfo.age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
        Constants.FIELD_CITY + "=" + userInfo.city + "|" +
        Constants.FIELD_SEX + "=" + userInfo.sex

      (sessionid, fullAggrInfo)
    }

    sessionid2FullAggrInfoRDD
  }


  /**
    * 根据日期获取对象的用户行为数据
    * @param spark
    * @param taskParam
    * @return
    */
  def getActionRDDByDateRange(spark: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    import spark.implicits._
    spark.sql("select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'")
      .as[UserVisitAction].rdd
  }

}
