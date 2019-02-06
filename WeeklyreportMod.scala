
import org.joda.time.DateTime.Property
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, DateTime, DateTimeZone}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import scala.collection.mutable.ListBuffer
import scala.util.Try

val confMap=sc.getConf.get("spark.driver.args").split("\\s+").map(x=>(x.split("=")(0),x.split("=")(1))).toMap
val weeklyDataOutputPath: String = confMap("weeklyDataPath")
val weeklyReportPath: String = confMap("weeklyReportPath")
val startOfWeek: String = confMap("startOfWeek")
val numOfWeeks: Int = confMap("numOfWeeks").toInt


def getWeekStart(startDate: String, numOfWeeks: Int): ListBuffer[String] ={
  val dates = new scala.collection.mutable.ListBuffer[String]
  val formatter = DateTimeFormat.forPattern("yyyyMMdd")
  val dt = formatter.parseDateTime(startDate)

  var currDate = dt
  for(i <- 1 to numOfWeeks){
    dates += currDate.toString(formatter)
    currDate = currDate.plusDays(7)
  }
  dates
}
object WeeklyStatus extends Enumeration {
  type WeeklyStatus = Value
  val created, lost, no_change = Value
}

val safeString: String => String = s => if (s == null) "" else s.trim()

val safeStringUDF = udf(safeString)

val safeWorkStatus: Int => Int = s => if (s == null) 1 else s

val safeWorkStatusUDF = udf(safeWorkStatus)

val safeActiveStatus: Int => Int = s => if (0 >= s) 0 else s

val safeActiveStatusUDF = udf(safeActiveStatus)

val safeCreated: Int => Int = s => if(s == 2) 1 else 0
val safeCreatedUDF = udf(safeCreated)

val safeLost: Int => Int = s => if(s == 3) 1 else 0
val safeLostUDF = udf(safeLost)

val safeLocation: String => String = s => if (s == null || s.isEmpty || s.equals("") || s.equals("UNKNOWN")) "unknown" else s.trim()
val safeLocationUDF = udf(safeLocation)



val filterLowCount: Int => Int = s => if(s == 0 ||  s > 50) s else -1

val filterLowCountUDF = udf(filterLowCount)

lazy val incomeUdf = udf((income: String) => { if(income == null || income.trim=="") "unknown" else if(income.trim == "$1,000 - $14,999" || income.trim=="$15,000 - $24,999")  "$1,000 - $24,999" else if(income.trim=="$25,000 - $34,999" || income.trim=="$35,000 - $49,999" || income.trim=="$50,000 - $74,999") "$25,000 - $74,999" else if( income.trim=="$75,000 - $99,999" || income.trim == "$175,000 - $199,999" || income.trim=="$150,000 - $174,999" || income.trim=="$125,000 - $149,999" || income.trim=="$100,000 - $124,999" || income.trim == "$200,000 - $249,999" )  "$75,000 - $249,999" else if(income.trim=="$250,000+") "$250,000+" else "unknown" })

lazy val ageUdf = udf((age: String) => { if(age == null || age.trim=="") "unknown" else if(age.trim == "18 - 24")  "18 - 24" else if(age.trim == "25 - 34")  "25 - 34" else if(age.trim == "35 - 44" || age.trim=="45 - 54")  "35 - 54" else if(age.trim == "55 - 64" || age.trim=="65 - 74" || age.trim=="75+")  "55+"  else "hello" })

lazy val filterCounts = udf((s1: String,s2:String) => {
  if  (s2.toInt>=50)
    s1
  else
    "-1"
})

val dates = getWeekStart(startOfWeek, numOfWeeks)


val zipMap = Map("01195"->"MA","01434"->"MA","02298"->"MA","03285"->"NH","03586"->"NH","03593"->"NH","03823"->"NH",
  "03861"->"NH","05408"->"VT","06042"->"CT","06338"->"CT","06461"->"CT","06792"->"CT","06824"->"CT","06825"->"CT",
  "06828"->"CT","06838"->"CT","06890"->"CT","07395"->"NJ","07699"->"NJ","10065"->"NY","10075"->"NY","10949"->"NY",
  "15289"->"PA","17015"->"PA","17202"->"PA","17408"->"PA","17622"->"PA","18302"->"PA","18902"->"PA","19060"->"PA",
  "19176"->"PA","19195"->"PA","19388"->"PA","20022"->"DC","20023"->"DC","20027"->"DC","20189"->"VA","20355"->"DC",
  "20509"->"DC","20511"->"DC","20528"->"DC","20529"->"DC","20598"->"VA","20810"->"MD","20811"->"MD","20993"->"MD",
  "21065"->"MD","21260"->"MD","21261"->"MD","21409"->"MD","22025"->"VA","22551"->"VA","22556"->"VA","23453"->"VA",
  "24205"->"VA","25403"->"WV","25404"->"WV","25405"->"WV","27395"->"NC","27497"->"NC","27523"->"NC","27527"->"NC",
  "27528"->"NC","27539"->"NC","27574"->"NC","27815"->"NC","28035"->"NC","28263"->"NC","28312"->"NC","28759"->"NC",
  "29021"->"SC","29707"->"SC","29907"->"SC","29909"->"SC","30073"->"GA","30112"->"GA","30169"->"GA","30270"->"GA",
  "30363"->"GA","30536"->"GA","31026"->"GA","31120"->"GA","31136"->"GA","31562"->"GA","31719"->"GA","31721"->"GA",
  "31788"->"GA","32006"->"FL","32026"->"FL","32081"->"FL","32723"->"FL","32885"->"FL","33222"->"FL","33336"->"FL",
  "33449"->"FL","33472"->"FL","33473"->"FL","33542"->"FL","33545"->"FL","33563"->"FL","33575"->"FL","33578"->"FL",
  "33579"->"FL","33596"->"FL","33646"->"FL","33812"->"FL","33929"->"FL","33966"->"FL","33967"->"FL","33973"->"FL",
  "33974"->"FL","33976"->"FL","34290"->"FL","34291"->"FL","34637"->"FL","34638"->"FL","34692"->"FL","34714"->"FL",
  "34715"->"FL","36421"->"AL","37544"->"TN","37934"->"TN","39813"->"GA","39815"->"GA","39817"->"GA","39818"->"GA",
  "39819"->"GA","39823"->"GA","39824"->"GA","39825"->"GA","39826"->"GA","39827"->"GA","39828"->"GA","39829"->"GA",
  "39832"->"GA","39834"->"GA","39836"->"GA","39837"->"GA","39840"->"GA","39841"->"GA","39842"->"GA","39845"->"GA",
  "39846"->"GA","39851"->"GA","39852"->"GA","39854"->"GA","39859"->"GA","39861"->"GA","39862"->"GA","39866"->"GA",
  "39867"->"GA","39870"->"GA","39877"->"GA","39885"->"GA","39886"->"GA","39897"->"GA","40598"->"KY","43069"->"OH",
  "43195"->"OH","45400"->"OH","46037"->"IN","46062"->"IN","46085"->"IN","46262"->"IN","47190"->"IN","48033"->"MI",
  "48168"->"MI","48193"->"MI","48480"->"MI","48638"->"MI","48855"->"MI","49037"->"MI","49519"->"MI","49528"->"MI",
  "49534"->"MI","50023"->"IA","50324"->"IA","52734"->"IA","53548"->"WI","53774"->"WI","54482"->"WI","55130"->"MN",
  "55467"->"MN","60124"->"IL","60169"->"IL","60290"->"IL","60296"->"IL","60297"->"IL","60403"->"IL","60404"->"IL",
  "60428"->"IL","60483"->"IL","60484"->"IL","60487"->"IL","60491"->"IL","60502"->"IL","60503"->"IL","60585"->"IL",
  "60586"->"IL","60642"->"IL","60682"->"IL","60686"->"IL","60689"->"IL","60695"->"IL","60958"->"IL","61705"->"IL",
  "62711"->"IL","62712"->"IL","63368"->"MO","64002"->"MO","65897"->"MO","67843"->"KS","68379"->"NE","70093"->"LA",
  "70595"->"LA","70891"->"LA","71217"->"LA","72019"->"AR","72198"->"AR","73025"->"OK","74019"->"OK","75156"->"TX",
  "75334"->"TX","75340"->"TX","75343"->"TX","75358"->"TX","75797"->"TX","76166"->"TX","77260"->"TX","77407"->"TX",
  "77498"->"TX","77523"->"TX","78541"->"TX","78542"->"TX","78574"->"TX","78633"->"TX","78798"->"TX","80023"->"CO",
  "80108"->"CO","80109"->"CO","80113"->"CO","80247"->"CO","80902"->"CO","80923"->"CO","80924"->"CO","80927"->"CO",
  "80938"->"CO","80939"->"CO","80951"->"CO","81403"->"CO","81507"->"CO","83414"->"WY","83646"->"ID","84005"->"UT",
  "84045"->"UT","84081"->"UT","84096"->"UT","85083"->"AZ","85097"->"AZ","85117"->"AZ","85118"->"AZ","85119"->"AZ",
  "85120"->"AZ","85121"->"AZ","85122"->"AZ","85123"->"AZ","85127"->"AZ","85128"->"AZ","85130"->"AZ","85131"->"AZ",
  "85132"->"AZ","85135"->"AZ","85137"->"AZ","85138"->"AZ","85139"->"AZ","85140"->"AZ","85141"->"AZ","85142"->"AZ",
  "85143"->"AZ","85145"->"AZ","85147"->"AZ","85172"->"AZ","85173"->"AZ","85178"->"AZ","85190"->"AZ","85191"->"AZ",
  "85192"->"AZ","85193"->"AZ","85194"->"AZ","85209"->"AZ","85238"->"AZ","85240"->"AZ","85243"->"AZ","85286"->"AZ",
  "85293"->"AZ","85294"->"AZ","85295"->"AZ","85298"->"AZ","85388"->"AZ","85392"->"AZ","85395"->"AZ","85396"->"AZ",
  "85658"->"AZ","85755"->"AZ","85756"->"AZ","85757"->"AZ","86315"->"AZ","86409"->"AZ","87144"->"NM","87151"->"NM",
  "87165"->"NM","88007"->"NM","88013"->"NM","88081"->"NM","89002"->"NV","89034"->"NV","89037"->"NV","89044"->"NV",
  "89054"->"NV","89081"->"NV","89085"->"NV","89105"->"NV","89136"->"NV","89140"->"NV","89157"->"NV","89161"->"NV",
  "89162"->"NV","89165"->"NV","89166"->"NV","89169"->"NV","89178"->"NV","89179"->"NV","89183"->"NV","89441"->"NV",
  "89460"->"NV","89508"->"NV","89519"->"NV","89521"->"NV","89555"->"NV","90090"->"CA","90189"->"CA","90755"->"CA",
  "90895"->"CA","91008"->"CA","92010"->"CA","92011"->"CA","92081"->"CA","92247"->"CA","92248"->"CA","92331"->"CA",
  "92344"->"CA","92395"->"CA","92617"->"CA","92725"->"CA","93036"->"CA","93290"->"CA","93314"->"CA","93475"->"CA",
  "93619"->"CA","93636"->"CA","93723"->"CA","93730"->"CA","94158"->"CA","94199"->"CA","94505"->"CA","94534"->"CA",
  "94582"->"CA","95467"->"CA","95757"->"CA","95811"->"CA","97086"->"OR","97089"->"OR","97239"->"OR","97317"->"OR",
  "97322"->"OR","97471"->"OR","98030"->"WA","98077"->"WA","98087"->"WA","98089"->"WA","98113"->"WA","98127"->"WA",
  "98139"->"WA","98141"->"WA","98165"->"WA","98175"->"WA","98194"->"WA","98213"->"WA","98229"->"WA","98391"->"WA",
  "98417"->"WA","98419"->"WA","98448"->"WA","98490"->"WA","98496"->"WA","99354"->"WA","99529"->"AK","99629"->"AK",
  "99729"->"AK","99566"->"AK","99573"->"AK","99574"->"AK","99755"->"AK","99588"->"AK","99743"->"AK","99901"->"AK",
  "99840"->"AK","99586"->"AK","99686"->"AK","06824"->"CT","06825"->"CT","06042"->"CT","06461"->"CT","06890"->"CT",
  "40231"->"KY","01434"->"MA","03861"->"NH","03823"->"NH","03593"->"NH","03586"->"NH","03285"->"NH","05408"->"VT")

val stateMap = Map("CT"->("New England","Northeast"),"ME"->("New England","Northeast"),"MA"->("New England","Northeast"),
  "NH"->("New England","Northeast"),"RI"->("New England","Northeast"),"VT"->("New England","Northeast"),"NJ"->("Mid-Atlantic","Northeast"),
  "NY"->("Mid-Atlantic","Northeast"),"PA"->("Mid-Atlantic","Northeast"),"IL"->("East North Central","Midwest"),
  "IN"->("East North Central","Midwest"),"MI"->("East North Central","Midwest"),"OH"->("East North Central","Midwest"),
  "WI"->("East North Central","Midwest"),"IA"->("West North Central","Midwest"),"KS"->("West North Central","Midwest"),
  "MN"->("West North Central","Midwest"),"MO"->("West North Central","Midwest"),"NE"->("West North Central","Midwest"),
  "ND"->("West North Central","Midwest"),"SD"->("West North Central","Midwest"),"DE"->("South Atlantic","South"),
  "FL"->("South Atlantic","South"),"GA"->("South Atlantic","South"),"MD"->("South Atlantic","South"),
  "NC"->("South Atlantic","South"),"SC"->("South Atlantic","South"),"VA"->("South Atlantic","South"),
  "DC"->("South Atlantic","South"),"WV"->("South Atlantic","South"),"AL"->("East South Central","South"),
  "KY"->("East South Central","South"),"MS"->("East South Central","South"),"TN"->("East South Central","South"),
  "AR"->("West South Central","South"),"LA"->("West South Central","South"),"OK"->("West South Central","South"),
  "TX"->("West South Central","South"),"AZ"->("Mountain","West"),"CO"->("Mountain","West"),"ID"->("Mountain","West"),
  "MT"->("Mountain","West"),"NV"->("Mountain","West"),"NM"->("Mountain","West"),"UT"->("Mountain","West"),
  "WY"->("Mountain","West"),"AK"->("Pacific","West"),"CA"->("Pacific","West"),"HI"->("Pacific","West"),"OR"->("Pacific","West"),"WA"->("Pacific","West"))


val zipDmaDf = spark.read.csv("/user/sjc/golden*/dictionary/verizon/zipdmastate").
  withColumnRenamed("_c0","dma_zip").withColumnRenamed("_c1","dma_code").
  withColumnRenamed("_c2","dma_state").withColumnRenamed("_c3","User_Demographic_Home_Dma").
  withColumnRenamed("_c4","dma_city")

val statDivRegDf = stateMap.map{case(key,value) =>(key,value._1, value._2)}.toSeq.toDF("state","User_Demographic_Home_Division","User_Demographic_Home_Region")

def groupByDemographic(df: DataFrame, level: String): DataFrame ={

  val demographicWithLocDf: DataFrame = df.groupBy("Payroll_Week", level).
    agg(count("subscriberid").alias("User_Total"),
      size(collect_set(when($"User_Demographic_Gender" === "M", $"subscriberid"))).alias("User_Demographic_gender_Male"),
      size(collect_set(when($"User_Demographic_Gender" === "F", $"subscriberid"))).alias("User_Demographic_gender_Female"),
      size(collect_set(when($"User_Demographic_Gender" === "", $"subscriberid"))).alias("User_Demographic_gender_Unknown"),
      size(collect_set(when($"User_Demographic_Age" === "18 - 24", $"subscriberid"))).alias("User_Demographic_Age_18_24"),
      size(collect_set(when($"User_Demographic_Age" === "25 - 34", $"subscriberid"))).alias("User_Demographic_Age_25_34"),
      size(collect_set(when($"User_Demographic_Age" === "35 - 54", $"subscriberid"))).alias("User_Demographic_Age_35_54"),
      size(collect_set(when($"User_Demographic_Age" === "55+", $"subscriberid"))).alias("User_Demographic_Age_55+"),
      size(collect_set(when($"User_Demographic_Age" === "unknown", $"subscriberid"))).alias("User_Demographic_Age_Unknown"),
      size(collect_set(when($"User_Demographic_Income" === "$1,000 - $24,999", $"subscriberid"))).alias("User_Demographic_Income_$1,000_$24,999"),
      size(collect_set(when($"User_Demographic_Income" === "$25,000 - $74,999", $"subscriberid"))).alias("User_Demographic_Income_$25,000_$74,999"),
      size(collect_set(when($"User_Demographic_Income" === "$75,000 - $249,999", $"subscriberid"))).alias("User_Demographic_Income_$75,000_$249,999"),
      size(collect_set(when($"User_Demographic_Income" === "$250,000+", $"subscriberid"))).alias("User_Demographic_Income_$250,000+"),
      size(collect_set(when($"User_Demographic_Income" === "unknown", $"subscriberid"))).alias("User_Demographic_Income_Unknown"),
      size(collect_set(when($"User_Demographic_Gender" === "M" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_gender_Male_Active"),
      size(collect_set(when($"User_Demographic_Gender" === "F" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_gender_Female_Active"),
      size(collect_set(when($"User_Demographic_Gender" === "" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_gender_Unknown_Active"),
      size(collect_set(when($"User_Demographic_Age" === "18 - 24" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Age_18_24_Active"),
      size(collect_set(when($"User_Demographic_Age" === "25 - 34" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Age_25_34_Active"),
      size(collect_set(when($"User_Demographic_Age" === "35 - 54" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Age_35_54_Active"),
      size(collect_set(when($"User_Demographic_Age" === "55+" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Age_55+_Active"),
      size(collect_set(when($"User_Demographic_Age" === "unknown" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Age_Unknown_Active"),
      size(collect_set(when($"User_Demographic_Income" === "$1,000 - $24,999" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Income_$1,000_$24,999_Active"),
      size(collect_set(when($"User_Demographic_Income" === "$25,000 - $74,999" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Income_$25,000_$74,999_Active"),
      size(collect_set(when($"User_Demographic_Income" === "$75,000 - $249,999" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Income_$75,000_$249,999_Active"),
      size(collect_set(when($"User_Demographic_Income" === "$250,000+" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Income_$250,000+_Active"),
      size(collect_set(when($"User_Demographic_Income" === "unknown" and $"active" === 1, $"subscriberid"))).alias("User_Demographic_Income_Unknown_Active"),
      round(sum(when($"active" === 1, $"user_weekly_working_hours"))).alias("User_Working_Hours"),
      round(sum(when($"User_Demographic_Gender" === "M" and $"active" === 1, $"user_weekly_working_hours"))).alias("User_Working_Hours_gender_Male"),
      round(sum(when($"User_Demographic_Gender" === "F" and $"active" === 1, $"user_weekly_working_hours"))).alias("User_Working_Hours_gender_Female"),
      round(sum(when($"User_Demographic_Gender" === "" and $"active" === 1, $"user_weekly_working_hours"))).alias("User_Working_Hours_gender_Unknown"),
      round(sum(when($"active" === 1, $"user_weekly_commuting_hours"))).alias("User_Commuting_Hours"),
      round(sum(when($"User_Demographic_Gender" === "M" and $"active" === 1, $"user_weekly_commuting_hours"))).alias("User_Commuting_Hours_gender_Male"),
      round(sum(when($"User_Demographic_Gender" === "F" and $"active" === 1, $"user_weekly_commuting_hours"))).alias("User_Commuting_Hours_gender_Female"),
      round(sum(when($"User_Demographic_Gender" === "" and $"active" === 1, $"user_weekly_commuting_hours"))).alias("User_Commuting_Hours_gender_Unknown"),
      sum(safeActiveStatusUDF(col("active"))).alias("User_Active_Workers_Total"),
      sum(safeCreatedUDF(col("new_work_status"))).alias("User_Workers_Created_Total"),
      sum(safeLostUDF(col("new_work_status"))).alias("User_Workers_Lost_Total"),
      sum(when($"User_Demographic_Gender" === "M",safeCreatedUDF(col("new_work_status")))).alias("User_Workers_Created_Total_gender_Male"),
      sum(when($"User_Demographic_Gender" === "F",safeCreatedUDF(col("new_work_status")))).alias("User_Workers_Created_Total_gender_Female"),
      sum(when($"User_Demographic_Gender" === "",safeCreatedUDF(col("new_work_status")))).alias("User_Workers_Created_Total_gender_Unknown"),
      sum(when($"User_Demographic_Gender" === "M",safeLostUDF(col("new_work_status")))).alias("User_Workers_Lost_Total_gender_Male"),
      sum(when($"User_Demographic_Gender" === "F",safeLostUDF(col("new_work_status")))).alias("User_Workers_Lost_Total_gender_Female"),
      sum(when($"User_Demographic_Gender" === "",safeLostUDF(col("new_work_status")))).alias("User_Workers_Lost_Total_gender_Unknown")
    )



  //  val withSafeCountsDf = demographicWithLocDf.withColumn("User_Active_Workers_Total_1", filterLowCountUDF(col("User_Active_Workers_Total"))).
  //    withColumn("User_Workers_Created_Total_1", filterLowCountUDF(col("User_Workers_Created_Total"))).
  //    withColumn("User_Workers_Lost_Total_1", filterLowCountUDF(col("User_Workers_Lost_Total"))).
  //    drop("User_Active_Workers_Total","User_Workers_Created_Total","User_Workers_Lost_Total").
  //    withColumnRenamed("User_Active_Workers_Total_1", "User_Active_Workers_Total").
  //    withColumnRenamed("User_Workers_Created_Total_1", "User_Workers_Created_Total").
  //    withColumnRenamed("User_Workers_Lost_Total_1", "User_Workers_Lost_Total")

  val withSafeCountsDf = demographicWithLocDf.
    withColumn("User_Total", filterLowCountUDF(col("User_Total"))).
    withColumn(level,safeLocationUDF(col(level))).
    withColumn("User_Demographic_gender_Male", filterLowCountUDF(col("User_Demographic_gender_Male"))).
    withColumn("User_Demographic_gender_Female", filterLowCountUDF(col("User_Demographic_gender_Female"))).
    withColumn("User_Demographic_gender_Unknown", filterLowCountUDF(col("User_Demographic_gender_Unknown"))).
    withColumn("User_Demographic_Age_18_24", filterLowCountUDF(col("User_Demographic_Age_18_24"))).
    withColumn("User_Demographic_Age_25_34", filterLowCountUDF(col("User_Demographic_Age_25_34"))).
    withColumn("User_Demographic_Age_35_54", filterLowCountUDF(col("User_Demographic_Age_35_54"))).
    withColumn("User_Demographic_Age_55+", filterLowCountUDF(col("User_Demographic_Age_55+"))).
    withColumn("User_Demographic_Age_Unknown", filterLowCountUDF(col("User_Demographic_Age_Unknown"))).
    withColumn("User_Demographic_Income_$1,000_$24,999", filterLowCountUDF(col("User_Demographic_Income_$1,000_$24,999"))).
    withColumn("User_Demographic_Income_$25,000_$74,999", filterLowCountUDF(col("User_Demographic_Income_$25,000_$74,999"))).
    withColumn("User_Demographic_Income_$75,000_$249,999", filterLowCountUDF(col("User_Demographic_Income_$75,000_$249,999"))).
    withColumn("User_Demographic_Income_$250,000+", filterLowCountUDF(col("User_Demographic_Income_$250,000+"))).
    withColumn("User_Demographic_Income_Unknown", filterLowCountUDF(col("User_Demographic_Income_Unknown"))).
    withColumn("User_Active_Workers_Total", filterLowCountUDF(col("User_Active_Workers_Total"))).
    withColumn("User_Workers_Created_Total", filterLowCountUDF(col("User_Workers_Created_Total"))).
    withColumn("User_Workers_Lost_Total", filterLowCountUDF(col("User_Workers_Lost_Total"))).
    withColumn("User_Demographic_gender_Male_Active", filterLowCountUDF(col("User_Demographic_gender_Male_Active"))).
    withColumn("User_Demographic_gender_Female_Active", filterLowCountUDF(col("User_Demographic_gender_Female_Active"))).
    withColumn("User_Demographic_gender_Unknown_Active", filterLowCountUDF(col("User_Demographic_gender_Unknown_Active"))).
    withColumn("User_Demographic_Age_18_24_Active", filterLowCountUDF(col("User_Demographic_Age_18_24_Active"))).
    withColumn("User_Demographic_Age_25_34_Active", filterLowCountUDF(col("User_Demographic_Age_25_34_Active"))).
    withColumn("User_Demographic_Age_35_54_Active", filterLowCountUDF(col("User_Demographic_Age_35_54_Active"))).
    withColumn("User_Demographic_Age_55+_Active", filterLowCountUDF(col("User_Demographic_Age_55+_Active"))).
    withColumn("User_Demographic_Age_Unknown_Active", filterLowCountUDF(col("User_Demographic_Age_Unknown_Active"))).
    withColumn("User_Demographic_Income_$1,000_$24,999_Active", filterLowCountUDF(col("User_Demographic_Income_$1,000_$24,999_Active"))).
    withColumn("User_Demographic_Income_$25,000_$74,999_Active", filterLowCountUDF(col("User_Demographic_Income_$25,000_$74,999_Active"))).
    withColumn("User_Demographic_Income_$75,000_$249,999_Active", filterLowCountUDF(col("User_Demographic_Income_$75,000_$249,999_Active"))).
    withColumn("User_Demographic_Income_$250,000+_Active", filterLowCountUDF(col("User_Demographic_Income_$250,000+_Active"))).
    withColumn("User_Demographic_Income_Unknown_Active", filterLowCountUDF(col("User_Demographic_Income_Unknown_Active"))).
    withColumn("User_Workers_Created_Total_gender_Male", filterLowCountUDF(col("User_Workers_Created_Total_gender_Male"))).
    withColumn("User_Workers_Created_Total_gender_Female", filterLowCountUDF(col("User_Workers_Created_Total_gender_Female"))).
    withColumn("User_Workers_Created_Total_gender_Unknown", filterLowCountUDF(col("User_Workers_Created_Total_gender_Unknown"))).
    withColumn("User_Workers_Lost_Total_gender_Male", filterLowCountUDF(col("User_Workers_Lost_Total_gender_Male"))).
    withColumn("User_Workers_Lost_Total_gender_Female", filterLowCountUDF(col("User_Workers_Lost_Total_gender_Female"))).
    withColumn("User_Workers_Lost_Total_gender_Unknown", filterLowCountUDF(col("User_Workers_Lost_Total_gender_Unknown")))





  withSafeCountsDf.select("Payroll_Week","User_Total",level,"User_Demographic_gender_Male","User_Demographic_gender_Female",
    "User_Demographic_gender_Unknown","User_Demographic_Age_18_24","User_Demographic_Age_25_34","User_Demographic_Age_35_54",
    "User_Demographic_Age_55+","User_Demographic_Age_Unknown","User_Demographic_Income_$1,000_$24,999",
    "User_Demographic_Income_$25,000_$74,999","User_Demographic_Income_$75,000_$249,999","User_Demographic_Income_$250,000+",
    "User_Demographic_Income_Unknown","User_Demographic_gender_Male_Active","User_Demographic_gender_Female_Active",
    "User_Demographic_gender_Unknown_Active","User_Demographic_Age_18_24_Active","User_Demographic_Age_25_34_Active",
    "User_Demographic_Age_35_54_Active","User_Demographic_Age_55+_Active","User_Demographic_Age_Unknown_Active",
    "User_Demographic_Income_$1,000_$24,999_Active","User_Demographic_Income_$25,000_$74,999_Active","User_Demographic_Income_$75,000_$249,999_Active",
    "User_Demographic_Income_$250,000+_Active","User_Demographic_Income_Unknown_Active","User_Active_Workers_Total",
    "User_Workers_Created_Total","User_Workers_Lost_Total","User_Workers_Created_Total_gender_Male",
    "User_Workers_Created_Total_gender_Female","User_Workers_Created_Total_gender_Unknown","User_Workers_Lost_Total_gender_Male",
    "User_Workers_Lost_Total_gender_Female","User_Workers_Lost_Total_gender_Unknown","User_Working_Hours","User_Working_Hours_gender_Male",
    "User_Working_Hours_gender_Female","User_Working_Hours_gender_Unknown","User_Commuting_Hours","User_Commuting_Hours_gender_Male",
    "User_Commuting_Hours_gender_Female","User_Commuting_Hours_gender_Unknown")

}

def writeAsText(df: DataFrame, path: String): Unit ={
  df.repartition(1).rdd.map(row => row.mkString("\t")).saveAsTextFile(path)
}

dates.foreach(dt =>{

  val weeklyDf = spark.read.parquet(weeklyDataOutputPath+"/"+dt).withColumn("age", ageUdf(col("User_Demographic_Age"))).
    withColumn("income", incomeUdf(col("User_Demographic_Income")))
  val activeStatusDf = weeklyDf.withColumn("new_work_status", safeWorkStatusUDF(weeklyDf("work_status"))).
    drop("User_Demographic_Age", "User_Demographic_Income").withColumnRenamed("age", "User_Demographic_Age").
    withColumnRenamed("income", "User_Demographic_Income")

  val dmaActiveStatusDf = activeStatusDf.join(zipDmaDf, $"User_Demographic_Home_Zip" === $"dma_zip" and $"User_Demographic_Home_State" === $"dma_state", "left")
  val divRegActiveStatusDf = activeStatusDf.join(statDivRegDf, $"User_Demographic_Home_State" === $"state", "left")

  val zipDemoResDf = groupByDemographic(activeStatusDf, "User_Demographic_Home_Zip")
  val dmaDemoResDf = groupByDemographic(dmaActiveStatusDf, "User_Demographic_Home_Dma")
  val stateDemoResDf = groupByDemographic(activeStatusDf, "User_Demographic_Home_State")
  val divDemoResDf = groupByDemographic(divRegActiveStatusDf, "User_Demographic_Home_Division")
  val regDemoResDef = groupByDemographic(divRegActiveStatusDf, "User_Demographic_Home_Region")

  //  println("activeStatusDf")
  //  activeStatusDf.show(2)
  //  println("zipDemoResDf")
  //  zipDemoResDf.select("User_Working_Hours","User_Commuting_Hours").show(2)
  //  zipDemoResDf.printSchema()
  //
  //  println("dmaDemoResDf")
  //  dmaDemoResDf.show(2)
  //
  //  println("stateDemoResDf")
  //  stateDemoResDf.show(2)
  //
  //  println("divDemoResDf")
  //  divDemoResDf.show(2)
  //
  //  println("regDemoResDef")
  //  regDemoResDef.show(2)
  //
  writeAsText(zipDemoResDf,weeklyReportPath+dt+"/zip_level")
  writeAsText(dmaDemoResDf,weeklyReportPath+dt+"/dma_level")
  writeAsText(stateDemoResDf,weeklyReportPath+dt+"/state_level")
  writeAsText(divDemoResDf,weeklyReportPath+dt+"/division_level")
  writeAsText(regDemoResDef,weeklyReportPath+dt+"/region_level")


})

sys.exit
