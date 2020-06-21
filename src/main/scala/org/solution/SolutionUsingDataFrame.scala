package org.solution

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.immutable.ListMap
import scala.util.control._
import java.util.Properties

object SolutionUsingDataFrame extends Context {
  def main(args: Array[String]): Unit = {
    sparkSession.sparkContext.setLogLevel("ERROR")

    val srcFilePath = "D:\\Data\\data.csv" //escape char required here
    val destFilePath = "fifa.parquet"
    val pgUrl = "jdbc:postgresql://[HOST_NAME]:[PORT]/[DATABASE_NAME]"
    val pgTable = "test_result"

    val pgProperties = new Properties()
    pgProperties.put("user", "postgres")
    pgProperties.put("password", "XXXXXX")
    pgProperties.put("driver", "org.postgresql.Driver")

    solution1(srcFilePath,destFilePath)
    solution2(destFilePath)
    solution3(destFilePath,pgUrl,pgProperties,pgTable)

    sparkSession.close()
  }

  def solution1(srcFilePath : String,destFilePath : String): Unit ={
    // Performing basic data cleaning operations like to converting Value & Wage to double datatype, removing invalid
    // charecters from the column names. After that writing the data into parquet file, so that better read performance
    // can be acheived and queries can performed faster.

    // reading from source file
    val dfSrcData = sparkSession
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("path", srcFilePath)
      .load()

    val df = dfSrcData
      .withColumn("Value", when(col("Value") === lit("0"), 0.0)
        .when(substring(col("Value"), -1, 1).contains("M"), regexp_extract(col("Value"), "\\d+(\\.\\d+)?", 0) * 1000)
        .otherwise(regexp_extract(col("Value"), "\\d+(\\.\\d+)?", 0)).cast(DoubleType))
      .withColumn("Wage", when(col("wage") === lit("0"), 0.0)
        .when(substring(col("Wage"), -1, 1).contains("M"), regexp_extract(col("Wage"), "\\d+(\\.\\d+)?", 0) * 1000)
        .otherwise(regexp_extract(col("Wage"), "\\d+(\\.\\d+)?", 0)).cast(DoubleType))
      .withColumnRenamed("Club Logo","Club_Logo")
      .withColumnRenamed("Preferred Foot", "Preferred_Foot")
      .withColumnRenamed("International Reputation","International_Reputation")
      .withColumnRenamed("Weak Foot","Weak_Foot")
      .withColumnRenamed("Skill Moves","Skill_Moves")
      .withColumnRenamed("Work Rate","Work_Rate")
      .withColumnRenamed("Body Type","Body_Type")
      .withColumnRenamed("Real Face","Real_Face")
      .withColumnRenamed("Jersey Number","Jersey_Number")
      .withColumnRenamed("Loaned From","Loaned_From")
      .withColumnRenamed("Contract Valid Until","Contract_Valid_Until")
      .withColumnRenamed("Release Clause","Release_Clause")

    // writing to parquet
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(destFilePath)

  }

  def solution2(destFilePath : String){
    val dfParquetSrc = sparkSession.read
      .format("parquet")
      .option("mergeSchema", "true")
      .option("path", destFilePath)
      .load()
    val dfFilterData = dfParquetSrc.select("Club", "Preferred_Foot", "Position", "Age", "Name", "Overall", "Value",
      "Wage", "Crossing", "Finishing", "HeadingAccuracy", "ShortPassing", "Volleys", "Dribbling", "Curve", "FKAccuracy",
      "LongPassing", "BallControl", "Acceleration", "SprintSpeed", "Agility", "Reactions", "Balance", "ShotPower",
      "Jumping", "Stamina", "Strength", "LongShots", "Aggression", "Interceptions", "Positioning", "Vision", "Penalties",
      "Composure", "Marking", "StandingTackle", "SlidingTackle", "GKDiving", "GKHandling", "GKKicking", "GKPositioning",
      "GKReflexes")
    dfFilterData.persist()

    // solution for Problem 2.a
    // Assumption is midfilders are playing in RWM RM RCM CM CAM CDM LCM LM LWM positions
    val dfLeftFootedMidfilders = dfFilterData.select("Club", "Preferred_Foot", "Position", "Age")
      .filter("(`Position`=='RWM' or `Position`=='RM' or `Position`=='RCM' or `Position`=='CM' or " +
        "`Position`=='CAM' or `Position`=='CDM' or `Position`=='LCM' or `Position`=='LM' or `Position`=='LWM') and " +
        "`Preferred_Foot`=='Left' and `Age`<30 and `Club`!='Null'")
      .groupBy("Club")
      .count()
      .withColumn("maxCount", max(col("count")).over(Window.partitionBy()))
      .filter(col("count") === col("maxCount"))
      .select("Club")
      .orderBy("Club")
    val clubsWithLeftFootedMidfilders = dfLeftFootedMidfilders.collect()
      .mkString(", ")
      .replace("[", "")
      .replace("]", "")
    println("Clubs having the most number of left footed midfielders under 30 years of age are " +
      clubsWithLeftFootedMidfilders + ".")

    // solution for Problem 2.b
    // Assumption is a player selected as per his best playing position (Position column)
    val dfPosition = dfFilterData.select("Club", "Name", "Overall", "Position")
      .filter("Position in ('GK','RB','CB','RCB','LCB','LB','RM','RWM','LCM','CM','RCM','LM','LWM','RF','CF','LF','ST') and Club!='Null'")
      .withColumn("modPos", when(col("Position") === lit("RWM"), "RM")
        .when(col("Position") === lit("LWM"), "LM")
        .when(col("Position") === lit("LCM"), "CM")
        .when(col("Position") === lit("RCB"), "CB")
        .when(col("Position") === lit("LCB"), "CB")
        .when(col("Position") === lit("RF"), "CF")
        .when(col("Position") === lit("LF"), "CF")
        .when(col("Position") === lit("RCM"), "CM")
        .when(col("Position") === lit("LCM"), "CM")
        .otherwise(col("Position")))
      .withColumn("rank", dense_rank().over(Window.partitionBy("Club", "modPos").orderBy(desc("Overall"))))
      .withColumn("selection", when(col("Position") === lit("GK") and col("rank") === lit(1), 1)
        .when(col("Position") === lit("RB") and col("rank") === lit(1), 1)
        .when(col("Position") === lit("LB") and col("rank") === lit(1), 1)
        .when(col("Position") === lit("ST") and col("rank") === lit(1), 1)
        .when(col("modPos") === lit("RM") and col("rank") === lit(1), 1)
        .when(col("modPos") === lit("LM") and col("rank") === lit(1), 1)
        .when(col("modPos") === lit("CF") and col("rank") === lit(1), 1)
        .when(col("modPos") === lit("CB") and col("rank") <= lit(2), 1)
        .when(col("modPos") === lit("CM") and col("rank") <= lit(2), 1)
        .otherwise(0))
      .filter("selection==1")
    val dfTeam = dfPosition.groupBy("Club")
      .avg("Overall")
      .orderBy(desc("avg(Overall)"))
    val bestTeamForPos = dfTeam.first()
    println("Strongest team by overall rating for a 4-4-2 formation is " + bestTeamForPos(0).toString + ".")

    // solution for Problem 2.c
    val dfClubValue = dfFilterData.select("Club", "Value", "Wage")
      .filter("Club!='Null'")
      .groupBy("Club")
      .agg(sum("Value"), sum("Wage"))
    dfClubValue.persist()
    val mostValuedTeam = dfClubValue.orderBy(desc("sum(Value)")).select("Club").first
    val mostWagedTeam = dfClubValue.orderBy(desc("sum(Wage)")).select("Club").first
    dfClubValue.unpersist()
    print(mostValuedTeam(0).toString + " has the most expensive squad value in the world. ")
    if (mostValuedTeam == mostWagedTeam) {
      println("That team also have the largest wage bill. ")
    } else {
      println("That team does not have the largest wage bill.")
    }

    // solution for Problem 2.d
    val mostWagedPosition = dfFilterData.select("Position", "Wage")
      .groupBy(col("Position"))
      .avg("Wage")
      .orderBy(desc("avg(Wage)"))
      .select("Position")
      .first()
    println("Position pays the highest wage in average is " + mostWagedPosition(0).toString + ".")

    // solution for Problem 2.e
    val gkSkills = dfFilterData.select("Overall", "Position", "Crossing", "Finishing", "HeadingAccuracy",
        "ShortPassing", "Volleys", "Dribbling", "Curve", "FKAccuracy", "LongPassing", "BallControl", "Acceleration",
        "SprintSpeed", "Agility", "Reactions", "Balance", "ShotPower", "Jumping", "Stamina", "Strength", "LongShots",
        "Aggression", "Interceptions", "Positioning", "Vision", "Penalties", "Composure", "Marking", "StandingTackle",
        "SlidingTackle", "GKDiving", "GKHandling", "GKKicking", "GKPositioning", "GKReflexes")
      .filter("Position='GK' and OverAll>=74")
      .drop("Overall", "Position")
      .groupBy()
      .avg()
    println("4 attributes which are most relevant to becoming a good goalkeeper are " + getTopColumns(gkSkills,4))

    // solution for Problem 2.f
    val stSkills = dfFilterData.select("Overall", "Position", "Crossing", "Finishing", "HeadingAccuracy",
      "ShortPassing", "Volleys", "Dribbling", "Curve", "FKAccuracy", "LongPassing", "BallControl", "Acceleration",
      "SprintSpeed", "Agility", "Reactions", "Balance", "ShotPower", "Jumping", "Stamina", "Strength", "LongShots",
      "Aggression", "Interceptions", "Positioning", "Vision", "Penalties", "Composure", "Marking", "StandingTackle",
      "SlidingTackle", "GKDiving", "GKHandling", "GKKicking", "GKPositioning", "GKReflexes")
      .filter("Position='ST' and OverAll>=74")
      .drop("Overall", "Position")
      .groupBy()
      .avg()
    println("5 attributes which are most relevant to becoming a top striker are " + getTopColumns(stSkills,5))

    dfFilterData.unpersist()

  }

  def solution3(destFilePath : String,pgUrl:String,pgProperties:Properties, tbl : String): Unit = {
    val dfParquetSrc = sparkSession.read
      .format("parquet")
      .option("mergeSchema", "true")
      .option("path", destFilePath)
      .load()
    val dfFilterData = dfParquetSrc.select("Overall", "Position", "Nationality", "Name", "Club", "Wage", "Value", "Joined", "Age")
        .withColumn("Joined",to_date(col("Joined"),"MMM dd,yyyy").as("Joined"))
        .withColumn("Wage",col("Wage")*lit(1000))
        .withColumn("Value",col("Value")*lit(1000))
        .withColumnRenamed("Nationality","Country")
        .withColumnRenamed("Overall","Overall Rating")
        .withColumnRenamed("Club","Club Name")
    // writing to postgres database
    dfFilterData.write
      .mode(SaveMode.Overwrite)
      .jdbc(url = pgUrl, table = tbl, connectionProperties = pgProperties)

  }

  def getTopColumns(df:DataFrame, num:Int): String ={
    // It returns high column name for Num rows from the dataframe.
    val columnMap = scala.collection.mutable.Map[String, Double]()
    var i = 0
    var output = ""
    val dfValues = df.collectAsList().get(0)
    val dfColumns = df.columns
    for(col <- dfColumns){
      columnMap(col) = dfValues.get(i).toString.toDouble
      i=i+1
    }
    val sortedMap = ListMap(columnMap.toSeq.sortWith(_._2 > _._2):_*)
    i=0
    val loop = new Breaks
    loop.breakable {
      for (out <- sortedMap) {
        output += out._1.replace("avg(","").replace(")","")
        i += 1
        if (i == num) {
          output += "."
          loop.break
        }else{
          output += ", "
        }
      }
    }
    output
  }


}
