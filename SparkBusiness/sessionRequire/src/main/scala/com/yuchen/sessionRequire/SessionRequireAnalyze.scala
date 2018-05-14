/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 17-11-13 下午6:47.
 * Author: lenovo.
 */

package com.yuchen.sessionRequire

import java.util.{Date, UUID}

import com.atguigu.commons.constant.Constants
import com.atguigu.commons.model.{UserInfo, UserVisitAction}
import com.atguigu.commons.utils.{DateUtils, NumberUtils, StringUtils, ValidUtils}
import com.yuchen.conf.SessionConfManager
import com.yuchen.conf.SessionConfManager.JsonParamAnalyze
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SessionRequireAnalyze {



  def main(args: Array[String]): Unit = {

    val taskParams = SessionConfManager.taskParams

    //创建spark conf
    val sparkConf = new SparkConf().setAppName("SessionRequireAnalyze").setMaster("local[*]")
    //创建spark session
    val ss = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //创建spark context
    val sc = ss.sparkContext


    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    //更具日期获取获取指定数据
    val actionByDateRdd = getActionRddByDate(ss,taskParams)


    //将用户行为转换成k-v结构
    val sessionActionRdd = actionByDateRdd.map(item =>(item.session_id,item))

    sessionActionRdd.persist(StorageLevel.MEMORY_ONLY)

    //将action颗粒度转换为session颗粒度，格式为<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
    val sessionRdd = Action2Session(ss,sessionActionRdd)

    //自定义累加器
    val accumulator = new SessionStatAccumulator

    //注册累加器
    sc.register(accumulator,"accumulator")

    val filterSesssionRdd = filterSession(sessionRdd,taskParams,accumulator)

    // 对数据进行内存缓存
    filterSesssionRdd.persist(StorageLevel.MEMORY_ONLY)

    // 业务功能一：统计各个范围的session占比，并写入MySQL
//    countSessionPercent(ss,accumulator.value,taskUUID)

    val sessiondetailRdd = getSessiondetailRdd(filterSesssionRdd,sessionActionRdd)
    sessiondetailRdd.persist(StorageLevel.MEMORY_ONLY)

    //业务二：随机均匀获取Session，之所以业务功能二先计算，是为了通过Action操作触发所有转换操作。
    randomSession(ss,taskUUID,filterSesssionRdd,sessiondetailRdd)


    // 关闭Spark上下文
    ss.close()

  }

  def randomSession(ss: SparkSession, taskUUID: String, filterSesssionRdd: RDD[(String, String)], sessiondetailRdd: RDD[(String, UserVisitAction)]) = {

    // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
    val timeSessionRdd = filterSesssionRdd.map{ case(sessionid,aggrInfo) =>
      val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
      val dateHour = DateUtils.getDateHour(startTime)
      (dateHour, aggrInfo)
    }

    //将<yyyy-MM-dd_HH,aggrInfo>转化为<yyyy-MM-dd_HH,count>
    val hourCountMap = timeSessionRdd.countByKey()

    // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引，
    // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
    hourCountMap.map{case(dateHour,count)=>
      val date = dateHour.split("_")(0)
      val hour  = dateHour.split("_")(1)
      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long](); dateHourCountMap(date) += (hour -> count)
        case Some(map) => map += (hour -> count)
      }
    }
    // 按时间比例随机抽取算法，总共要抽取100个session，先按照天数，进行平分
    val extractNumberPerDay = 100 / dateHourCountMap.size

    //获取每天没小时随机取样的个数<yyyy-MM-dd,<HH,num>>
    val extractNumberPerDayHour = new mutable.HashMap[String, mutable.HashMap[String, Int]]()
    dateHourCountMap.map{ case(date,hourCountMap)=>
      // 计算出这一天的session总数
      val sessionCount = hourCountMap.values.sum
      val hourNumMap = hourCountMap.map{ case(hour,count) =>
        val num = (count / sessionCount * extractNumberPerDay).toInt
        (hour,num)
      }
      extractNumberPerDayHour.get(date) match{
        case None => extractNumberPerDayHour(date) =  hourNumMap
        case Some(map) => hourNumMap
      }
    }

    val extractNumberPerDayHourBroadcast = ss.sparkContext.broadcast(extractNumberPerDayHour)

    // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
    val time2sessionsRDD = timeSessionRdd.groupByKey()

    val dateSessionInfoArray = new mutable.HashMap[String, mutable.HashMap[String,Iterable[String]]]()

    time2sessionsRDD.foreach{ case(dateHour,sessionInfo) =>
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      dateSessionInfoArray.get(date) match {
        case None => dateSessionInfoArray(date) = new mutable.HashMap[String,Iterable[String]]()
          dateSessionInfoArray(date) += (hour -> sessionInfo)
        case Some(map) => map += (hour -> sessionInfo)
      }
    }

    val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()

    val randomTemp = dateSessionInfoArray.flatMap{ case(date,hourArray) =>
      hourArray.map{ case (hour,sessionItera) =>
          val array = sessionItera.toArray
          val arrayRdd = ss.sparkContext.makeRDD(array)
          val hourNum = extractNumberPerDayHourBroadcast.value.get(date).get(hour)
          arrayRdd.takeSample(false,hourNum).map{sessionInfo =>
            val sessionid = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_SESSION_ID)
            val starttime = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategoryIds = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            sessionRandomExtractArray += SessionRandomExtract(taskUUID, sessionid, starttime, searchKeywords, clickCategoryIds)
          }
      }
      sessionRandomExtractArray
    }

    //随机取样数据

    val sessionRandomExtract = ss.sparkContext.makeRDD(randomTemp.toArray)

    // 将抽取后的数据保存到MySQL
    import ss.implicits._
    sessionRandomExtract.toDF().write
      .format("jdbc")
      .option("url", SessionConfManager.jdbcURL)
      .option("dbtable", "session_random_extract")
      .option("user", SessionConfManager.jdbcUSR)
      .option("password", SessionConfManager.jdbcPW)
      .mode(SaveMode.Append)
      .save()

    val extractSessionidsRDD = sessionRandomExtract.map(item => (item.sessionid, item.sessionid))

    // 第四步：获取抽取出来的session的明细数据
    val extractSessionDetailRDD = extractSessionidsRDD.join(sessiondetailRdd)

    val sessionDetailRDD = extractSessionDetailRDD.map { case (sid, (sessionid, userVisitAction)) =>
      SessionDetail(taskUUID, userVisitAction.user_id, userVisitAction.session_id,
        userVisitAction.page_id, userVisitAction.action_time, userVisitAction.search_keyword,
        userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
        userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
    }

    // 将明细数据保存到MySQL中
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", SessionConfManager.jdbcURL)
      .option("dbtable", "session_detail")
      .option("user", SessionConfManager.jdbcUSR)
      .option("password", SessionConfManager.jdbcPW)
      .mode(SaveMode.Append)
      .save()
  }


  def getSessiondetailRdd(filterSesssionRdd: RDD[(String, String)], sessionActionRdd: RDD[(String, UserVisitAction)]): RDD[(String, UserVisitAction)] = {
    filterSesssionRdd.join(sessionActionRdd).map(item => (item._1, item._2._2))
  }


  /**
    * 业务需求一：计算各session范围占比，并写入MySQL
    *
    */

  def countSessionPercent(ss: SparkSession, value: mutable.HashMap[String, Int], taskUUID: String): Unit = {
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

    import ss.implicits._
    val sessionAggrStatRDD = ss.sparkContext.makeRDD(Array(sessionAggrStat))

    sessionAggrStatRDD
      .toDF()
      .write
      .format("jdbc")
      .option("url", SessionConfManager.jdbcURL) //(Constants.JDBC_URL)
      .option("dbtable", "session_aggr_stat")
      .option("user", SessionConfManager.jdbcUSR)
      .option("password", SessionConfManager.jdbcPW)
      .mode(SaveMode.Append)
      .save()
  }


  def filterSession(sessionRdd: RDD[(String, String)], taskParams: JsonParamAnalyze, accumulator: SessionStatAccumulator): RDD[(String, String)] = {
    // 获取查询任务中的配置
    val startAge = taskParams.getParam(Constants.PARAM_START_AGE)
    val endAge = taskParams.getParam(Constants.PARAM_END_AGE)
    val professionals = taskParams.getParam(Constants.PARAM_PROFESSIONALS)
    val cities = taskParams.getParam(Constants.PARAM_CITIES)
    val sex = taskParams.getParam(Constants.PARAM_SEX)
    val keywords = taskParams.getParam(Constants.PARAM_KEYWORDS)
    val categoryIds = taskParams.getParam(Constants.PARAM_CATEGORY_IDS)

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
    val filterSessionRdd = sessionRdd.filter { case (sessionid, aggrInfo) =>
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
    
      if(success){
        accumulator.add(Constants.SESSION_COUNT)
        
        //统计各分组访问时间的次数
        def calculateVisitTimes(length:Long): Unit ={
          if (length >= 1 && length <= 3) {
            accumulator.add(Constants.TIME_PERIOD_1s_3s);
          } else if (length >= 4 && length <= 6) {
            accumulator.add(Constants.TIME_PERIOD_4s_6s);
          } else if (length >= 7 && length <= 9) {
            accumulator.add(Constants.TIME_PERIOD_7s_9s);
          } else if (length >= 10 && length <= 30) {
            accumulator.add(Constants.TIME_PERIOD_10s_30s);
          } else if (length > 30 && length <= 60) {
            accumulator.add(Constants.TIME_PERIOD_30s_60s);
          } else if (length > 60 && length <= 180) {
            accumulator.add(Constants.TIME_PERIOD_1m_3m);
          } else if (length > 180 && length <= 600) {
            accumulator.add(Constants.TIME_PERIOD_3m_10m);
          } else if (length > 600 && length <= 1800) {
            accumulator.add(Constants.TIME_PERIOD_10m_30m);
          } else if (length > 1800) {
            accumulator.add(Constants.TIME_PERIOD_30m);
          }
        }
        
        //统计访问步长的次数
        def calculateStepTime(length:Long): Unit ={
          if (length >= 1 && length <= 3) {
            accumulator.add(Constants.STEP_PERIOD_1_3);
          } else if (length >= 4 && length <= 6) {
            accumulator.add(Constants.STEP_PERIOD_4_6);
          } else if (length >= 7 && length <= 9) {
            accumulator.add(Constants.STEP_PERIOD_7_9);
          } else if (length >= 10 && length <= 30) {
            accumulator.add(Constants.STEP_PERIOD_10_30);
          } else if (length > 30 && length <= 60) {
            accumulator.add(Constants.STEP_PERIOD_30_60);
          } else if (length > 60) {
            accumulator.add(Constants.STEP_PERIOD_60);
          }
        }

        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
        val visitLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        val stepLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
        calculateVisitTimes(visitLength)
        calculateStepTime(stepLength)
      }
      success
    }
    
    filterSessionRdd
  }


  def getActionRddByDate(ss: SparkSession, taskParams:  JsonParamAnalyze): RDD[UserVisitAction] = {
    val startDate = taskParams.getParam(Constants.PARAM_START_DATE)
    val endDate = taskParams.getParam(Constants.PARAM_END_DATE)

    import ss.implicits._
    ss.sql("select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'")
      .as[UserVisitAction].rdd
  }

  def Action2Session(ss: SparkSession, sessionActionRdd: RDD[(String, UserVisitAction)]): RDD[(String, String)] = {
    val sessionGroupRdd = sessionActionRdd.groupByKey()

    val sessionAggrGroupRdd = sessionGroupRdd.map { case (sessionId, userActions) =>
      val searchKeywordsBuffer = new StringBuffer("")
      val clickCategoryIdsBuffer = new StringBuffer("")
      var userid = -1L

      // session的起始和结束时间
      var startTime: Date = null
      var endTime: Date = null

      // session的访问步长
      var stepLength = 0

      // 遍历session所有的访问行为
      userActions.foreach{userAction =>
        if (userid == -1L) {
          userid = userAction.user_id
        }
        val searchKeyword = userAction.search_keyword
        val clickCategoryId = userAction.click_category_id

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
        val actionTime = DateUtils.parseTime(userAction.action_time)

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
      val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

      (userid, partAggrInfo);
    }

    // 查询所有用户数据，并映射成<userid,Row>的格式
    import ss.implicits._
    val userRdd = ss.sql("select * from user_info").as[UserInfo].rdd.map(item => (item.user_id, item))

    // 将session粒度聚合数据，与用户信息进行join
    val userFullInfoRdd = sessionAggrGroupRdd.join(userRdd);

    val sessionFullInfoRdd = userFullInfoRdd.map { case (uid, (partAggrInfo, userInfo)) =>
      val sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)

      val fullAggrInfo = partAggrInfo + "|" +
        Constants.FIELD_AGE + "=" + userInfo.age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
        Constants.FIELD_CITY + "=" + userInfo.city + "|" +
        Constants.FIELD_SEX + "=" + userInfo.sex

      (sessionid, fullAggrInfo)
    }
    sessionFullInfoRdd
  }

}
