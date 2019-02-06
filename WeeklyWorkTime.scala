import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable.ListBuffer



val confMap = sc.getConf.get("spark.driver.args").split("\\s+").map(x=>(x.split("=")(0),x.split("=")(1))).toMap
val dailyDataOutputPath: String = confMap("dailyDataPath")
val weeklyDataOutputPath: String = confMap("weeklyDataPath")
//val weeklyChurnedOutputPath : String = confMap("weeklyChurnedPath")
//val weeklyNewUserOutputPath: String = confMap("weeklyNewUserPath")
val startOfWeek: String = confMap("startOfWeek")
val numOfWeeks: Int = confMap("numOfWeeks").toInt


def getWeeks(inpDate: String, numOfWeeks: Int): (ListBuffer[(String,String)]) ={
  val dates = new scala.collection.mutable.ListBuffer[(String,String)]
  val formatter = DateTimeFormat.forPattern("yyyyMMdd")
  val dt = formatter.parseDateTime(inpDate)
  var currentDt = dt
  for(i <- 1 to numOfWeeks){
    val startDate = currentDt.toString(formatter)
    val endDate = currentDt.plusDays(6).toString(formatter)
    dates += ((startDate, endDate))
    currentDt = currentDt.plusDays(7)
  }
  dates
}

def getDatesBetween(start: String, end: String): ListBuffer[String] ={
  val dates = new ListBuffer[String]()
  val formatter = DateTimeFormat.forPattern("yyyyMMdd")
  val st: DateTime = formatter.parseDateTime(start)
  val et: DateTime = formatter.parseDateTime(end)
  val numberOfDays = Days.daysBetween(st, et).getDays
  for(i<- 0 to numberOfDays){
    dates += st.plusDays(i).toString(formatter)
  }
  dates
}

def getHour(minutes: Double): Long ={
  val hours: Double = minutes / 60
  hours.round
}

val getHourUDF = udf[Long, Double](getHour)
val dataWeeks: ListBuffer[(String, String)] = getWeeks(startOfWeek, numOfWeeks)


def compareWeeklyStatus(prevEmp: Int, currEmp: Int): Int = {
  if(prevEmp == -1 || prevEmp == null){
    1 //same
  }
  else if(prevEmp.equals(currEmp)){
    1 //same
  }else if (prevEmp.equals(1) && currEmp.equals(0)){
    3 //lost
  }else if(prevEmp.equals(0) && currEmp.equals(1)){
    2  // added
  }else{
    1 // same
  }

}

val compareWeeklyStatusUDF = udf[Int, Int, Int](compareWeeklyStatus)

def generateWorkStatus(df: DataFrame): DataFrame = {
  val resDf = if(df.head(1).isEmpty){
    spark.emptyDataFrame
  }else{
    // This means this value is seed, belongs to initial 4 weeks of data.
    df.withColumn("work_status", lit(0)).withColumn("employed", lit(-1))
  }
  resDf
}

def ifEmployed(prevEmp: Int, currEmp: Int): Int ={
  if(prevEmp == null){
    -1
  }
  else if(currEmp == -1 && prevEmp!= -1 || currEmp == null && prevEmp!= -1){
    prevEmp
  }else{
    currEmp
  }
}

val validEmploymentUDF = udf[Int, Int, Int](ifEmployed)

def getEmploymentStatus(groupedRows: Iterator[Row]): Int ={
  var statusCounter = 0
  var paww = 0L
  var itrCnt = 0
  //Must have 4 status to decide employment status
  groupedRows.foreach(row =>{
    itrCnt += 1
    if(row.getAs[Int]("active") == 1){
      statusCounter += 1
      paww += row.getAs[Long]("present_at_work_weekly")
    }
  })
  var employed = -1
  if(itrCnt == 4 && statusCounter == 0){
    employed = 0
  }else if(paww >= 5){
    employed = 1
  }

  employed
}

def compareWeeklyDf(df1: DataFrame, df2: DataFrame, df3: DataFrame, df4: DataFrame): DataFrame ={
  val week = df1.head().getAs[String]("Payroll_Week")
  val allDf = df1.union(df2.drop("work_status", "employed")).
    union(df3.drop("work_status", "employed")).union(df4.drop("work_status", "employed")) //.union(df5.drop("work_status", "employed"))
  println("allDf")
  //  allDf.show()
  val weeklActiveyDf = allDf.select("subscriberid", "active", "present_at_work_weekly")
    .groupByKey(row =>{row.getAs[String]("subscriberid")}).mapGroups{case(id, rows: Iterator[Row]) =>{
    val subid = id

    val empStatus = getEmploymentStatus(rows)
    (id, empStatus)
  }}toDF("subscriberid","employed")
  println("weeklActiveyDf")
  //  weeklActiveyDf.show()
  println("weeklyDf")
  val weeklyDf = allDf.join(weeklActiveyDf, Seq("subscriberid"), "left").filter($"Payroll_Week" === week)
  //  weeklyDf.show(2)
  //  weeklyDf.show(2)
  //TODO rename "employed" to "employed_prev"
  val joinDf = weeklyDf.join(df2.withColumnRenamed("employed","employed_prev").select("subscriberid","employed_prev"), Seq("subscriberid"), "left")
  //  println("joinDf")
  //  joinDf.show(2)
  val resDf = joinDf.withColumn("work_status", compareWeeklyStatusUDF(col("employed_prev"), col("employed"))).withColumn("employed",validEmploymentUDF(col("employed_prev"), col("employed"))).drop("employed_prev")
  //  val resDf = df1.join(tempDf.select("subscriberid","work_status","employed"), Seq("subscriberid"), "left")
  //  println("resDf")
  //  resDf.show(2)
  resDf
}

def getPresentAtWork(workingMinutes: Double): Int ={
  val whr = getHour(workingMinutes)
  if(whr >= 1D){
    1
  }else 0
}

val presentAtWorkUDF = udf[Int, Double](getPresentAtWork)

//@deprecated
//def getActiveUser(workingHours: Double): Int ={
//  if(workingHours > 1D){
//    1
//  }else 0
//}
//@deprecated
//val getActiveUserUDF = udf[Int, Double](getActiveUser)

def getActiveUser(paw: Int): Int ={
  if(paw >= 1){
    1
  }else 0
}

val getActiveUserUDF = udf[Int, Int](getActiveUser)

val safeString: String => String = s => if (s == null) "" else s.trim()

val safeStringUDF = udf(safeString)

//  var preWeekDf: DataFrame = spark.emptyDataFrame

dataWeeks.foreach(wk =>{
  val formatter = DateTimeFormat.forPattern("yyyyMMdd")
  val dates = getDatesBetween(wk._1, wk._2)
  var weekDf: DataFrame = spark.emptyDataFrame


  dates.foreach(dt =>{

    val opFormatter = DateTimeFormat.forPattern("MM-dd-yyyy")
    val weekDate = formatter.parseDateTime(wk._1)
    println(s"WEEK DATE $weekDate")
    val week = weekDate.toString(opFormatter)
    println(s"WEEK $week")
    val dailyStr = scala.util.Try(spark.read.parquet(dailyDataOutputPath+"/date="+dt))
    val tempdailyRes: DataFrame = if(dailyStr.isSuccess){dailyStr.get}else{spark.emptyDataFrame}
    // filter out on minutes > 960 (16 hours it is outlier case most likely becaz of error in logic)
    //    println("tempdailyRes")
    //    tempdailyRes.show(2)

    if(!tempdailyRes.head(1).isEmpty){
      val dailyRes = tempdailyRes.filter(col("working_minutes") < 960)
      val dailyDf: DataFrame = dailyRes.withColumn("Payroll_Week", lit(s"$week")).withColumn("present_at_work", presentAtWorkUDF(col("working_minutes")))
      //      println("dailyDf")
      //      dailyDf.show(2)
      if(!weekDf.head(1).isEmpty){
        weekDf = weekDf.union(dailyDf)
      }else{
        weekDf = dailyDf
      }

    }
  })
  println("WEEK DF")
  //      weekDf.show(2)
  val workDf: DataFrame = weekDf.groupBy("subscriberid", "Payroll_Week").
    agg(getHourUDF(sum(weekDf("working_minutes"))).alias("user_weekly_working_hours"),
      getHourUDF(sum(weekDf("commuting_minutes"))).alias("user_weekly_commuting_hours"),
      safeStringUDF(first("age")).alias("User_Demographic_Age"),
      safeStringUDF(first("gender")).alias("User_Demographic_Gender"),
      safeStringUDF(first("income")).alias("User_Demographic_Income"),
      safeStringUDF(first("education")).alias("User_Demographic_Education"),
      safeStringUDF(first("home_value")).alias("User_Demographic_Home_Value"),
      safeStringUDF(first("occupation")).alias("User_Demographic_Occupation"),
      safeStringUDF(first("home_zip")).alias("User_Demographic_Home_Zip"),
      safeStringUDF(first("home_state")).alias("User_Demographic_Home_State"),
      sum(col("present_at_work")).alias("present_at_work_weekly"))
  val activeDf = workDf.withColumn("active", getActiveUserUDF(col("present_at_work_weekly")))
  println("activeDf")
  //  activeDf.show(2)
  //    activeDf.persist(StorageLevel.MEMORY_AND_DISK)
  val pre2WeekDate =  formatter.parseDateTime(wk._1).minusDays(7)
  //  println(s"pre2WeekDate $pre2WeekDate")
  val pre2Res = scala.util.Try(spark.read.parquet(weeklyDataOutputPath+"/"+pre2WeekDate.toString(formatter)))
  val pre2WeekDf: DataFrame = if(pre2Res.isSuccess){pre2Res.get}else{spark.emptyDataFrame}
  //  pre2WeekDf.show(2)
  val pre3WeekDate =  formatter.parseDateTime(wk._1).minusDays(14)
  //  println(s"pre3WeekDate $pre3WeekDate")
  val pre3Res = scala.util.Try(spark.read.parquet(weeklyDataOutputPath+"/"+pre3WeekDate.toString(formatter)))
  val pre3WeekDf: DataFrame = if(pre3Res.isSuccess){pre3Res.get}else{spark.emptyDataFrame}
  //  pre3WeekDf.show(2)
  val pre4WeekDate =  formatter.parseDateTime(wk._1).minusDays(21)
  //  println(s"pre4WeekDate $pre4WeekDate")
  val pre4Res = scala.util.Try(spark.read.parquet(weeklyDataOutputPath+"/"+pre4WeekDate.toString(formatter)))
  val pre4WeekDf: DataFrame = if(pre4Res.isSuccess){pre4Res.get}else{spark.emptyDataFrame}
  //  pre4WeekDf.show(2)
  //
  val pre5WeekDate =  formatter.parseDateTime(wk._1).minusDays(28)
  //  println(s"pre5WeekDate $pre5WeekDate")
  val pre5Res = scala.util.Try(spark.read.parquet(weeklyDataOutputPath+"/"+pre5WeekDate.toString(formatter)))
  val pre5WeekDf: DataFrame = if(pre5Res.isSuccess){pre5Res.get}else{spark.emptyDataFrame}
  //  pre5WeekDf.show(2)
  //

  val weeklyDF = if(!pre2WeekDf.head(1).isEmpty && !pre3WeekDf.head(1).isEmpty && !pre4WeekDf.head(1).isEmpty){
    println("COMPARING WITH LAST 3 WEEKS DATA")
    compareWeeklyDf(activeDf, pre2WeekDf, pre3WeekDf, pre4WeekDf)
  }else{
    val subscriberid = $"subscriberid".string
    val dfSchema = StructType(subscriberid :: Nil)
    generateWorkStatus(activeDf)
  }
  //    println("WEEKLY DF")
  //  weeklyDF.show(10)
  weeklyDF.write.parquet(weeklyDataOutputPath+"/"+wk._1)
  //  churnedDf.write.parquet(weeklyChurnedOutputPath+"/"+wk._1)
  //  addDf.write.parquet(weeklyNewUserOutputPath+"/"+wk._1)
  //    spark.catalog.clearCache()
})

sys.exit
