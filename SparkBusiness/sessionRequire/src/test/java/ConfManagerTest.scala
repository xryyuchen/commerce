import com.atguigu.commons.utils.StringUtils
import com.yuchen.conf.SessionConfManager

/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 17-11-14 下午2:07.
 * Author: lenovo.
 */

object ConfManagerTest {

  def main(args: Array[String]): Unit = {

    val bool = StringUtils.isEmpty(SessionConfManager.taskParams.getParam("cities"))
    println( bool)

  }

}
