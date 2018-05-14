import com.atguigu.commons.model.{ProductInfo, UserInfo, UserVisitAction}
import com.yuchen.conf.SessionConfManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 17-11-14 上午11:31.
 * Author: lenovo.
 */

object SqlTest {


  def main(args: Array[String]): Unit = {

    //创建spark conf
    val sparkConf = new SparkConf().setAppName("SessionRequireAnalyze").setMaster("local[*]")
    //创建spark session
    val ss = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import ss.implicits._
    ss.sql("select * from user_visit_action limit 100").as[UserVisitAction]
      .write.json(SessionConfManager.locaPath + "/user_visit_action")


    ss.sql("select * from user_info limit 100").as[UserInfo]
      .write.json(SessionConfManager.locaPath + "/user_info")

    ss.sql("select * from product_info").as[ProductInfo]
      .write.json(SessionConfManager.locaPath + "/product_info")

    ss.stop()

  }

}
