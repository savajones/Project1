import java.sql.{Connection, DriverManager}
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
    val combinecsv: DataFrame =spark.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/Resources/CombineData.csv")
    combinecsv.createOrReplaceTempView("2019NFLCombine")

  def adminm(): Unit ={
    println("ADMIN:\n" +
      "[1] Make new ADMIN\n" +
      "[2] Select query to analyze\n" +
      "[3] Update username\n" +
      "[4] Update password\n" +
      "[5] Logout")
    val adminc = readInt()
  }

  def basicm(): Unit ={
    println("BASIC:\n" +
      "[1] Select query to analyze\n" +
      "[2] Update username\n" +
      "[3] Update password\n" +
      "[4] Logout")
    val basicc = readInt()
  }

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
      case 2 =>
    }
  }
}
