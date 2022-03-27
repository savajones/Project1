import CombineAnalysis.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import scala.io.StdIn.readLine
import scala.io.StdIn.readInt

object CombineAnalysis {

  System.setProperty("hadoop.home.dir", "~/hadoop")
  val spark: SparkSession = SparkSession
    .builder
    .appName("2019 NFL Combine Analysis")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val combinecsv: DataFrame =spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/Resources/CombineData.csv")
  combinecsv.createOrReplaceTempView("2019NFLCombine")
  spark.sql("SET spark.hadoop.hive.exec.dynamic.partition = true")
  spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
  val userscsv: DataFrame =spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/Resources/Users.csv")
  userscsv.createOrReplaceTempView("Users")

  /*----ADMIN MENU----*/
  def adminm(): Unit ={
    println("ADMIN:\n" +
      "[1] Make new ADMIN\n" +
      "[2] Show all users\n" +
      "[3] Select query to analyze\n" +
      "[4] Update query\n" +
      "[5] Update username\n" +
      "[6] Update password\n" +
      "[7] Logout")
    val adminc = readInt()
    adminc match{
      case 1 =>

      case 2 =>
        spark.sql("SELECT * FROM Users").show()
      case 3 =>

      case 4 =>

      case 5 =>

      case 6 =>

      case 7 =>

    }
  }

  /*----BASIC MENU----*/
  def basicm(): Unit ={
    println("BASIC:\n" +
      "[1] Select query to analyze\n" +
      "[2] Update username\n" +
      "[3] Update password\n" +
      "[4] Logout")
    val basicc = readInt()
    basicc match{
      case 1 =>
        println(
          "[1] Average 40yd,Vertical,Bench,Broad Jump,3Cone,Shuttle\n" +
          "[2] Top 10 athletes in each event\n" +
          "[3] Min and max of each event\n" +
          "[4] Number of different schools that got drafted\n" +
          "[5] School with the most draft picks\n" +
          "[6] Clemson athlete draft picks\n")
        val basicq = readInt()
        basicq match{
          case 1 =>
            println(
              "[1] Overall average\n" +
              "[2] Position average\n"
            )
            val average = readInt()
            average match {
              case 1 =>

              case 2 =>
                println(
                  "What position?\n" +
                  "S,LB,OT,WR,RB,CB,OL,TE,DL,QB"
                )
                val position = readLine()
            }
        }
      case 2 =>
      case 3 =>
      case 4 =>
    }
  }

  /*----NEW USER----*/
  def newusr(answer: Int) ={
    println("Enter your first name:\n")
    val fn = readLine()
    println("Create a username:\n")
    val un = readLine()
    println("Create a password:\n")
    val pw = readLine()
    println("Account created successfully\n")
    (fn,un,pw)
  }

  /*----QUERIES----*/
  def queries(): Unit ={
    /*----Average 40yd,Vertical,Bench,Broad Jump,3Cone,Shuttle----*/
    spark.sql("SELECT AVG(40yd) AS 40yd,AVG(Vertical) AS Vertical,AVG(Bench) AS Bench,AVG(3Cone) AS 3Cone,AVG(Shuttle) AS Shuttle FROM 2019NFLCombine").show()
    /*----Top 10 athletes in each event----*/
    spark.sql("")
    /*----Min and max of each event----*/
    spark.sql("")
    /*----Number of different schools that got drafted----*/
    spark.sql("")
    /*----School with the most draft picks----*/
    spark.sql("")
    /*----Clemson athlete draft picks----*/
    spark.sql("")
  }

  /*----LOGIN----*/
  def login(username: String,password: String) ={

  }
  spark.sql("SELECT AVG(40yd) AS 40yd,AVG(Vertical) AS Vertical,AVG(Bench) AS Bench,AVG(3Cone) AS 3Cone,AVG(Shuttle) AS Shuttle FROM 2019NFLCombine").show()
  def main(args: Array[String]): Unit ={
    println(Console.RED+"\n███████████████████████████████████████████████████████████████████████████████████████\n█▀▄▄▀█─▄▄─█▀░██░▄▄░███▄─▀█▄─▄█▄─▄▄─█▄─▄█████─▄▄▄─█─▄▄─█▄─▀█▀─▄█▄─▄─▀█▄─▄█▄─▀█▄─▄█▄─▄▄─█\n██▀▄██─██─██░██▄▄▄░████─█▄▀─███─▄████─██▀███─███▀█─██─██─█▄█─███─▄─▀██─███─█▄▀─███─▄█▀█\n▀▄▄▄▄▀▄▄▄▄▀▄▄▄▀▄▄▄▄▀▀▀▄▄▄▀▀▄▄▀▄▄▄▀▀▀▄▄▄▄▄▀▀▀▄▄▄▄▄▀▄▄▄▄▀▄▄▄▀▄▄▄▀▄▄▄▄▀▀▄▄▄▀▄▄▄▀▀▄▄▀▄▄▄▄▄▀")
    println(Console.RESET)
    println("Login or create an account to view an analysis of the 2019 NFL Combine")
    println("[1] Login \n" +
      "[2] Create an account")
    val user = readInt()
    user match{
      case 1 =>
        println("Enter username:\n")
        val username = readLine()

        println("Enter password:\n")
        val password = readLine()
      case 2 =>

    }


  }
}
