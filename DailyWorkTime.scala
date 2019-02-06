

import org.joda.time.DateTime.Property
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, DateTime, DateTimeZone}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import scala.collection.mutable.ListBuffer
import scala.util.Try
import util.control.Breaks._



val confMap=sc.getConf.get("spark.driver.args").split("\\s+").map(x=>(x.split("=")(0),x.split("=")(1))).toMap
//  val dateOfTheMonth = confMap("monthdate")
val dailyDataOutputPath: String = confMap("dailyData")
// location buffer. we normally dont get signal from work but place close to it
val startDate: String = confMap("startdate")
val endDate: String = confMap("enddate")
val locSignalBuffStr: String = confMap("locSignalBuff")
val staticDataJoinerPath = confMap("staticouput")
val homeLunchPath = confMap("homelunchoutput")
val locationPath = confMap("locationoutput")


val locSignalBuff =   locSignalBuffStr.toInt

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

def converTimeZone(shortId: String): String ={
  val timeZoneMap =  Map(
    "AKD" -> "US/Alaska",
    "AKS" -> "US/Alaska",
    "AKDT" -> "US/Alaska",
    "AKST" -> "US/Alaska",
    "CDT" -> "America/Chicago",
    "EDT" -> "US/Eastern",
    "HST" -> "US/Hawaii",
    "HDT" -> "US/Hawaii",
    "HAST" -> "US/Hawaii",
    "HADT" -> "US/Hawaii",
    "SST" -> "US/Samoa",
    "SDT" -> "US/Samoa",
    "MDT" -> "America/Denver",
    "PDT" -> "America/Los_Angeles",
    "EST" -> "US/Eastern",
    "CST" -> "America/Chicago",
    "MST" -> "America/Denver",
    "PST" -> "America/Los_Angeles"
  )
  timeZoneMap.get(shortId).getOrElse("America/Los_Angeles")
}

val converTimeZoneUDF = udf[String, String](converTimeZone)



def toDateTime(timeStamp: Long, timeZone: String): Long = {
  new DateTime(timeStamp * 1000, DateTimeZone.forID(timeZone)).getMillis
}

val toDateTimeUDF = udf[Long, Long, String](toDateTime)

def toDayOfMonth(timeStamp: Long, timeZone: String): String = {
  new DateTime(timeStamp * 1000, DateTimeZone.forID(timeZone)).dayOfWeek().getAsText()
}

val toDayOfMonthUDF = udf[String, Long, String](toDayOfMonth)

def toWeekOfYear(timeStamp: Long, timeZone: String): String = {
  val res: DateTime = new DateTime(timeStamp * 1000, DateTimeZone.forID(timeZone)).weekOfWeekyear().withMinimumValue()
  val formatter = DateTimeFormat.forPattern("yyyyMMdd")
  res.toString(formatter)
}

val toWeekOfYearUDF = udf[String, Long, String](toWeekOfYear)

def getActiveUser(workingMinutes: Double): Int ={
  if(workingMinutes > 1D){
    1
  }else 0
}

val getActiveUserUDF = udf[Int, Double](getActiveUser)

object WeeklyStatus extends Enumeration {
  type WeeklyStatus = Value
  val created, lost, no_change = Value
}

def compareWeeklyStatus(status1: Int, status2: Int): String = {
  if(status1.equals(status2)){
    WeeklyStatus.no_change.toString
  }else if (status1.equals(1) && status2.equals(0)){
    WeeklyStatus.lost.toString
  }else{
    WeeklyStatus.created.toString
  }

}

val compareWeeklyStatusUDF = udf[String, Int, Int](compareWeeklyStatus)


def compareWeeklyDf(df1: DataFrame, df2: DataFrame): DataFrame ={
  val modDf = df1.select("subscriberid", "active").join(df2,df1("subscriberid") === df2("subscriberid") ,"full_outer").
    withColumn("prev_active", df1("active")).drop(df1("active")).drop(df1("subscriberid"))

  modDf.withColumn("work_status", compareWeeklyStatusUDF(modDf("prev_active"),modDf("active")))
}

val compareWeeklyUDF = (a: DataFrame, b: DataFrame) => compareWeeklyDf(a,b)

val dataDates = getDatesBetween(startDate, endDate)

//case class Location(mdn_hash: String, loc_lon: Double, loc_lat: Double, loc_startDateTime: String, loc_startTime: Long, midm_starttime: Long, midm_duration: Long, timeZone: String)

///////////////////////////time calc start/////////////////////////////

def calculateWorkHours(locStatDf: DataFrame, dt: String) = {
  val tempGr = locStatDf.groupByKey(row => (row.getAs[String]("subscriberid"), row.getAs[String]("day_of_month")))
  val dailyMinutesDf = tempGr.mapGroups{case((id,day),rows) => {
    def getDistance(firstLat: Double, firstLong: Double, secondLat: Double, secondLong: Double): Double = {
      val radiusOfEarthInMiles = 3959D
      val rightAngleInDegrees = 90.0
      val straightAngleInDegrees = 180.0
      val cosineThreshold = 1.0
      val degreesToRadians = Math.PI / straightAngleInDegrees
      val phi1 = (rightAngleInDegrees - firstLat) * degreesToRadians
      val phi2 = (rightAngleInDegrees - secondLat) * degreesToRadians
      val theta1 = firstLong * degreesToRadians
      val theta2 = secondLong * degreesToRadians
      val arc: Double = Math.acos(Math.min(cosineThreshold, Math.sin(phi1) * Math.sin(phi2) * Math.cos(theta1-theta2)
        + Math.cos(phi1) * Math.cos(phi2)))
      arc * radiusOfEarthInMiles * 1609.34 //converting to meters
    }

    //      def isBucket11_4Window(local_time: DateTime): Boolean = {
    //        if (local_time.getHourOfDay() >= 11 && local_time.getHourOfDay() <= 16) {
    //          return true;
    //        }
    //        else {
    //          return false;
    //        }
    //      }

    val sortedByTimestamp: List[Row] = rows.toList.sortBy(_.getAs[Long]("startTime"))
    var home_morning_timestamp: DateTime = new DateTime(0L)
    var work_first_timestamp: DateTime = new DateTime(0L)
    var work_leaving_timestamp: DateTime = new DateTime(0L)
    var home_return_timestamp: DateTime = new DateTime(0L)
    var homeLeftReachedOffice: Boolean = false
    var workLeftLastMarked: Boolean = false
    val age = sortedByTimestamp(0).getAs[String]("resolved_set_age")
    val gender = sortedByTimestamp(0).getAs[String]("resolved_set_gender")
    val income = sortedByTimestamp(0).getAs[String]("experian_income")
    val education = sortedByTimestamp(0).getAs[String]("experian_education")
    val homeValue = sortedByTimestamp(0).getAs[String]("experian_home_value")
    val occupation = sortedByTimestamp(0).getAs[String]("experian_occupation")
    val homeLat = sortedByTimestamp(0).getAs[Double]("home_medLat")
    val homeLon = sortedByTimestamp(0).getAs[Double]("home_medLong")
    val workLat = sortedByTimestamp(0).getAs[Double]("lunch_medLat")
    val workLon = sortedByTimestamp(0).getAs[Double]("lunch_medLong")
    val homeZip = sortedByTimestamp(0).getAs[String]("home_zip")
    val homeState = sortedByTimestamp(0).getAs[String]("home_state")
    val week = sortedByTimestamp(0).getAs[String]("week_of_year")
    var firstWork = true
    var firstWorkTimestamp: DateTime =  new DateTime(0L)
    var home_last_timestamp: DateTime = new DateTime(0L)
    var home_current_timestamp: DateTime = new DateTime(0L)
    var home_first_timestamp: DateTime = new DateTime(0L)
    val homeToWorkDistance = getDistance(homeLat, homeLon, workLat, workLon)
    val locPrStr: String = sortedByTimestamp(0).getAs[String]("precision")
    val locPrecison = if(null == locPrStr || locPrStr.isEmpty || locPrStr.equals("null")){1000D}else{locPrStr.toDouble}
    val acceptedDistance = locPrecison + locSignalBuff
    val acceptedDistance2 = locPrecison + 3500
    val hwDistanceFlag: Boolean = if(acceptedDistance < homeToWorkDistance){true}else{false}
    val locationBuffer = new ListBuffer[Long]()

    sortedByTimestamp.foreach(row =>{
      val userLat = if(null != row.getAs[Double]("lat")){row.getAs[Double]("lat")}else{0D}
      val userLon = if(null != row.getAs[Double]("lon")){row.getAs[Double]("lon")}else{0D}
      val localTime = new DateTime(row.getAs[Long]("local_time"))

      val userWorkDistance = getDistance(workLat, workLon, userLat, userLon)
      val userHomeDistance = getDistance(homeLat, homeLon, userLat, userLon)


      if(userWorkDistance <= acceptedDistance && hwDistanceFlag && !localTime.isEqual(0L)){

        if(firstWork){
          firstWorkTimestamp = localTime
          firstWork = false
        }

        if(work_first_timestamp.getMillis == 0L){
          work_first_timestamp = localTime
          home_first_timestamp = home_current_timestamp
        }
        work_leaving_timestamp = localTime
      }

      if (userHomeDistance <= acceptedDistance2 && hwDistanceFlag && !localTime.isEqual(0L)) {
        home_current_timestamp = localTime
        locationBuffer += localTime.getMillis; // it mantain all home time stamps.
      }
    })
    breakable{
      locationBuffer.foreach(x => {
        if (x > work_leaving_timestamp.getMillis){
          // get first home timestamp after work leaving timestamp
          home_last_timestamp = new DateTime(x)
          break
        }
      })


    }

    var timeAtHome = 0D
    var morningCommute = 0D
    var eveningCommute = 0D
    var commuteTime = 0D

    if (home_last_timestamp.getMillis != 0L && home_first_timestamp.getMillis != 0L && work_leaving_timestamp.getMillis != 0) {

      timeAtHome = (home_last_timestamp.getMillis - home_first_timestamp.getMillis).toDouble /(1000 * 60)
      morningCommute = (work_first_timestamp.getMillis -  home_first_timestamp.getMillis).toDouble/(1000 * 60)
      eveningCommute = (home_last_timestamp.getMillis - work_leaving_timestamp.getMillis).toDouble/(1000 * 60)
      commuteTime = morningCommute + eveningCommute
      //      coverage =1;

    }


    val workDuration: Double = if(!work_first_timestamp.getMillis.equals(0L)){((work_leaving_timestamp.getMillis - work_first_timestamp.getMillis).toDouble / (1000*60))}else{0D}
    val workingMinutes: Double = workDuration //if(workDuration> 30D){workDuration}else{0D}

    (id, workingMinutes, commuteTime, age, gender, income, education, homeValue, occupation, homeZip, homeState)
  }}.toDF("subscriberid", "working_minutes", "commuting_minutes", "age", "gender",
    "income", "education", "home_value", "occupation", "home_zip", "home_state")

  println("DAILY PATH"+dailyDataOutputPath+dt)
  dailyMinutesDf.write.parquet(dailyDataOutputPath+"="+ dt)


}

///////////////////////////time cal end///////////////////////////////

dataDates.foreach(dt =>{

  val statRes  = scala.util.Try(spark.read.parquet("/processed/staticDataJoin/20181003").
    select("subscriberid", "resolved_set_age", "experian_education",
      "experian_home_value", "experian_income", "resolved_set_gender", "experian_occupation"))
  val statDf: DataFrame = if(statRes.isSuccess){statRes.get}else{spark.emptyDataFrame}
  // statDf.show(2)


  val homeLunch = scala.util.Try(spark.read.parquet(homeLunchPath).
    select("mdn_hash", "home_zip", "home_medLat", "home_medLong", "home_confRadius", "home_confLevel",
      "lunch_medLat", "lunch_medLong", "lunch_confRadius", "lunch_confLevel", "lunch_zip", "home_state"))
  val homeLunchDf = if(homeLunch.isSuccess){homeLunch.get}else{spark.emptyDataFrame}

  val locPath = locationPath+dt+"/workflowTime=*/"
  val locRes = scala.util.Try(spark.read.parquet(locPath).filter(array_contains(col("status"), "BMI")).
    select("mdn_hash", "lon", "lat", "startDateTime", "startTime", "precision", "timeZone"))
  println(locRes.isSuccess)
  val locMidDf = if(locRes.isSuccess){locRes.get}else{spark.emptyDataFrame}
  val locDf = locMidDf.withColumn("tz", converTimeZoneUDF(locMidDf("timeZone"))).drop("timeZone").withColumnRenamed("tz", "timeZone")
  // locDf.show(2)

  val locStatDf  = locDf.join(statDf, locDf("mdn_hash") === statDf("subscriberid"), "left_outer").drop(col("mdn_hash")).
    withColumn("local_time", toDateTimeUDF(locDf("startTime"), locDf("timeZone"))).
    withColumn("day_of_month", toDayOfMonthUDF(locDf("startTime"), locDf("timeZone"))).
    withColumn("week_of_year", toWeekOfYearUDF(locDf("startTime"), locDf("timeZone")))

  val locStatHlDf = locStatDf.join(homeLunchDf, locStatDf("subscriberid") === homeLunchDf("mdn_hash")).drop(col("mdn_hash"))

  calculateWorkHours(locStatHlDf, dt)

})

sys.exit
