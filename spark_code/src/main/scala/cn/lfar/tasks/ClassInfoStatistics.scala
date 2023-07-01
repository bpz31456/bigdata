package cn.lfar.tasks

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ClassInfoStatistics {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val session: SparkSession = SparkSession.builder()
      .appName("ClassInfoStatistics")
      .master("local[*]")
      .getOrCreate();

    val sparkContext: SparkContext = session.sparkContext

    //1. 读取文件的数据test.txt
    val classInfoRDD: RDD[String] =
      sparkContext.textFile(this.getClass.getClassLoader.getResource("test.txt").getPath)
        .zipWithIndex().filter(_._2 > 0).map(_._1)

    import session.implicits._
    val df: DataFrame = classInfoRDD.map(_.split(" "))
      .map(x => ClassInfo(x(0).toInt, x(1), x(2).toInt, x(3), x(4), x(5).toInt)).toDF()
    df.createTempView("class_info")


    /*  这里会把数据写到mysql中去
        val rst: DataFrame = session.sql("select * from class_info")
        val properties = new Properties()
        properties.setProperty("user", "root")
        properties.setProperty("password", "123456")
        rst.write.mode(SaveMode.Append).jdbc(url = "jdbc:mysql://bigdata03:3306/mydb?characterEncoding=utf-8&useSSL=false", "class_info", properties)

    */

    //查询所有学生信息
    session.sql("select * from class_info").show

    //2. 一共有多少个小于20岁的人参加考试？
    println(s"2. 一共有多少个小于20岁的人参加考试？")
    session.sql("select count(1) from class_info where age <20").show


    //3. 一共有多少个等于20岁的人参加考试？
    println(s"3. 一共有多少个等于20岁的人参加考试？")
    session.sql("select count(1) from class_info where age = 20").show

    //4. 一共有多少个大于20岁的人参加考试？
    println("4. 一共有多少个大于20岁的人参加考试？")
    session.sql("select count(1) from class_info where age = 20").show

    //5. 一共有多个男生参加考试？
    println("5. 一共有多个男生参加考试？")
    session.sql("select count(1) from class_info where sex='男'").show


    //6. 一共有多少个女生参加考试？
    println("6. 一共有多少个女生参加考试？")
    session.sql("select count(1) from class_info where sex='女'").show
    //7. 12班有多少人参加考试？
    println("7. 12班有多少人参加考试？")
    session.sql("select count(1) from class_info where clazz=12").show

    //8. 13班有多少人参加考试？
    println("8. 13班有多少人参加考试？")
    session.sql("select count(1) from class_info where clazz=13").show

    //9. 语文科目的平均成绩是多少？
    println("9. 语文科目的平均成绩是多少？")
    session.sql("select avg(score) from class_info where subject='chinese'").show

    //10. 数学科目的平均成绩是多少？
    println("10. 数学科目的平均成绩是多少？")
    session.sql("select avg(score) from class_info where subject='math'").show

    //11. 英语科目的平均成绩是多少？
    println("11. 英语科目的平均成绩是多少？")
    session.sql("select avg(score) from class_info where subject = 'english'").show

    //12. 每个人平均成绩是多少？
    println("12. 每个人平均成绩是多少？")
    session.sql("select name,avg(score) from class_info group by name").show

    //13. 12班平均成绩是多少？
    println("13. 12班平均成绩是多少？")
    session.sql("select clazz,avg(score) from class_info where clazz=12 group by clazz").show

    //14. 12班男生平均总成绩是多少？
    println("14. 12班男生平均总成绩是多少？")
    session.sql("select clazz,sex,avg(score) from class_info where clazz=12 and sex='男' group by clazz,sex").show

    //15. 12班女生平均总成绩是多少？
    println("15. 12班女生平均总成绩是多少？")
    session.sql("select clazz,sex,avg(score) from class_info where clazz=12 and sex='女' group by clazz,sex").show

    //16. 13班平均成绩是多少？
    println("16. 13班平均成绩是多少？")
    session.sql("select clazz,avg(score) from class_info where clazz=13 group by clazz").show

    //17. 13班男生平均总成绩是多少？
    println("17. 13班男生平均总成绩是多少？")
    session.sql("select clazz,sex,avg(score) from class_info where clazz=13 and sex='男' group by clazz,sex").show

    //18. 13班女生平均总成绩是多少？
    println("18. 13班女生平均总成绩是多少？")
    session.sql("select clazz,sex,avg(score) from class_info where clazz=13 and sex='女' group by clazz,sex").show

    //19. 全校语文成绩最高分是多少？
    println("19. 全校语文成绩最高分是多少？")
    session.sql("select subject,max(score) from class_info where subject='chinese' group by subject").show

    //20. 12班语文成绩最低分是多少？
    println("20. 12班语文成绩最低分是多少？")
    session.sql("select clazz,subject,min(score) from class_info where subject='chinese' and clazz=12 group by clazz, subject").show

    //21. 13班数学最高成绩是多少？
    println("21. 13班数学最高成绩是多少？")
    session.sql("select clazz, subject,max(score) from class_info where subject='math' and clazz=13 group by clazz, subject").show

    //22. 总成绩大于150分的12班的女生有几个？
    println("22. 总成绩大于150分的12班的女生有几个？==这个还要再处理")
    session.sql("select clazz,sex,count(tmp.name) from (select clazz,sex,name,avg(score) from class_info where clazz=12 and sex='女' group by clazz,sex,name having sum(score)>150) tmp " +
      "group by clazz,sex").show

    //23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
    println("23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？")
    session.sql("select a.name,avg(a.score) from class_info a " +
      "join (select b.name from class_info b where b.age>=19 and b.score>=70 and b.subject='math') tmp1 on a.name=tmp1.name " +
      "join (select c.name from class_info c group by c.name having sum(c.score)>150) tmp2 on a.name=tmp2.name " +
      "group by a.name").show

    session.close()
  }

}

//班级  姓名  年龄  性别  科目 成绩
case class ClassInfo(clazz: Int, name: String, age: Int, sex: String, subject: String, score: Int)