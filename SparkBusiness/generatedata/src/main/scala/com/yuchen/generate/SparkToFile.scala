/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 17-11-13 下午2:23.
 * Author: lenovo.
 */

package com.yuchen.generate

import java.io.FileInputStream
import java.util.{Properties, UUID}

import com.atguigu.commons.model.{ProductInfo, UserInfo, UserVisitAction}
import com.atguigu.commons.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SparkToFile {

  /**
    * 模拟用户行为信息
    *
    * @return
    */
  private def mockUserVisitActionData(): Array[UserVisitAction] = {

    val searchKeywords = Array("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val date = DateUtils.getTodayDate()
    val actions = Array("search", "click", "order", "pay")
    val random = new Random()
    val rows = ArrayBuffer[UserVisitAction]()

    for (i <- 0 to 100) {
      val userid = random.nextInt(100)
      for (j <- 0 to 10) {
        val sessionid = UUID.randomUUID().toString().replace("-", "")
        val baseActionTime = date + " " + random.nextInt(23)
        var clickCategoryId: Long = -1L

        for (k <- 0 to random.nextInt(100)) {
          val pageid = random.nextInt(10)
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword: String = null
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityid = random.nextInt(10).toLong
          val action = actions(random.nextInt(4))

          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(10))
            case "click" => if (clickCategoryId == null) clickCategoryId = String.valueOf(random.nextInt(100)).toLong
              clickProductId = String.valueOf(random.nextInt(100)).toLong
            case "order" => orderCategoryIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            case "pay" => payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString
          }

          rows += UserVisitAction(date, userid, sessionid,
            pageid, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds, cityid)

        }

      }

    }
    rows.toArray
  }

  /**
    * 模拟用户信息表
    *
    * @return
    */
  private def mockUserInfo(): Array[UserInfo] = {

    val rows = ArrayBuffer[UserInfo]()
    val sexes = Array("male", "female")
    val random = new Random()

    for (i <- 0 to 100) {
      val userid = i
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60)
      val professional = "professional" + random.nextInt(100)
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(2))
      rows += UserInfo(userid, username, name, age,
        professional, city, sex)
    }
    rows.toArray
  }

  /**
    * 模拟产品数据表
    *
    * @return
    */
  private def mockProductInfo(): Array[ProductInfo] = {

    val rows = ArrayBuffer[ProductInfo]()
    val random = new Random()
    val productStatus = Array(0, 1)

    for (i <- 0 to 100) {
      val productId = i
      val productName = "product" + i
      val extendInfo = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"

      rows += ProductInfo(productId, productName, extendInfo)
    }

    rows.toArray
  }


  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val sparkConf = new SparkConf().setAppName("sparktofile").setMaster("local[*]");

    // 创建SparkSession客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc =  spark.sparkContext

    // 模拟数据
    val userVisitActionData = this.mockUserVisitActionData()
    val userInfoData = this.mockUserInfo()
    val productInfoData = this.mockProductInfo()

    val userVisitActionDataRdd = sc.makeRDD(userVisitActionData);
    val userInfoDataRdd = sc.makeRDD(userInfoData)
    val productInfoDataRdd = sc.makeRDD(productInfoData)

    val prop = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("sparkFile.properties").getPath

    prop.load(new FileInputStream(path));
    val localpath = prop.getProperty("localpath")

//    import spark.implicits._
//    userInfoDataRdd.toDF()

    userInfoDataRdd.map(x => {
      x.user_id+","+x.username+","+x.name+","+x.age+","+x.professional+","+x.city+","+x.sex
    }).saveAsTextFile(localpath+"/user")

    sc.stop()
    spark.stop()


  }


}
