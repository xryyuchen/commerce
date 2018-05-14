import com.yuchen.sessionRequire.SessionStatAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 17-11-13 下午7:35.
 * Author: lenovo.
 */


//{startDate:"2017-10-20", \
//  endDate:"2017-10-31", \
//  startAge: 20, \
//  endAge: 50, \
//  professionals: "",  \
//  cities: "", \
//  sex:"", \
//  keywords:"", \
//  categoryIds:"", \
//  targetPageFlow:"1,2,3,4,5,6,7"}


case class TaskParams(startDate:String,
                      endDate:String,
                      startAge:String,
                      endAge:String,
                      professionals:String,
                      cities:String,
                      sex:String,
                      keywords:String,
                      categoryIds:String,
                      targetPageFlow:String)

object TestJson {



  def main(args: Array[String]): Unit = {

//      val params = SessionConfManager.taskParams
//    val str = params.getParam("startDate")
//    println(str)

    val sparkConf = new SparkConf().setAppName("SessionRequireAnalyze").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val arr1 = Array(("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),
      ("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),
      ("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),
      ("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),
      ("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),
      ("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),
      ("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),
      ("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),
      ("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),
      ("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),
      ("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),
      ("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),
      ("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),
      ("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),
      ("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),("k3",1),("k1",1),("k2",1),
      ("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1),("k1",1),("k4",1),("k3",1))
    val rdd = sc.makeRDD(arr1)
    val accumulator = new SessionStatAccumulator

    // 注册自定义累加器
    sc.register(accumulator,"accumulator")

    rdd.map{
      case(k,v) => {
        accumulator.add(k)
      }
    }.count()
    println(rdd.getNumPartitions)
    println(accumulator.value)



  }
}
