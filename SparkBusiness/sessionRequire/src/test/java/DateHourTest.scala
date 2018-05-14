import scala.collection.mutable

/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 17-11-15 上午9:23.
 * Author: lenovo.
 */

object DateHourTest {

  def main(args: Array[String]): Unit = {

    val hourCountMap = Map(
      ("2017-11-31_11",1),("2017-11-31_11",1),("2017-11-31_12",1),("2017-11-31_12",1),("2017-11-31_12",1),("2017-11-31_13",1),
      ("2017-12-01_11",1),("2017-12-01_14",1),("2017-12-01_14",1),("2017-12-01_11",1),("2017-12-01_11",1),("2017-12-01_15",1),
      ("2017-11-11_11",1),("2017-11-11_14",1),("2017-11-11_17",1),("2017-11-11_17",1),("2017-11-11_17",1),("2017-11-11_17",1))

    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    hourCountMap.map{case(dateHour,count)=>
      val date = dateHour.split("_")(0)
      val hour  = dateHour.split("_")(1)
      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long](); dateHourCountMap(date) += (hour -> count)
        case Some(hourCountMap) => hourCountMap += (hour -> count)
      }
    }
    println(dateHourCountMap)
  }


}
