import CombineAnalysis.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import scala.annotation.tailrec
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
  spark.sql("DROP Table IF EXISTS Users")
  spark.sql("CREATE TABLE Users(Name String,Username String,Password String) PARTITIONED BY(Permission String)")
  spark.sql("INSERT INTO TABLE Users VALUES('Savannah','sjones','ssj1','ADMIN'),('Dave','davidw','passw0rd','BASIC')")

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
        /*----NEW ADMIN----*/
        println("Enter your first name:\n")
        val fna = readLine()
        println("Create a username:\n")
        val una = readLine()
        println("Create a password:\n")
        val pwa = readLine()
        println("ADMIN created successfully\n")
        spark.sql(s"INSERT INTO Users VALUES('$fna','$una','$pwa','ADMIN')")
        adminm()
      case 2 =>
        /*----SHOW USERS----*/
        spark.sql("SELECT * FROM Users").show()
        adminm()
      case 3 =>
        /*----QUERIES----*/
        queries()
        adminm()
      case 4 =>
        /*----UPDATE QUERY----*/
        println("Enter a different school to see their draft picks:\n")
        val diffs = readLine()
        spark.sql(s"SELECT Player,School,Drafted AS Draft_Pick FROM 2019NFLCombine WHERE School='$diffs' AND Drafted IS NOT NULL").show(1000,1000,false)
        println(
          "Would you like to save as a json file?\n" +
            "[1] Yes\n" +
            "[2] No\n"
        )
        val json = readInt()
        json match{
          case 1 =>
            {spark.sql(s"SELECT Player,School,Drafted AS Draft_Pick FROM 2019NFLCombine WHERE School='$diffs' AND Drafted IS NOT NULL").write.format("org.apache.spark.sql.json").mode("overwrite").save("src/main/Data")
            println("Saved to src/main/Data")}
            adminm()
          case 2 =>
            println("Returning to ADMIN menu...")
            adminm()
        }
      case 5 =>
        /*----UPDATE USERNAME----*/
        println("Please verify your login info:\n" +
          "First Name:\n")
        val firstname1 = readLine()
        println("Username:\n")
        val usernameu1 = readLine()
        println("Password:\n")
        val passwordu1 = readLine()
        println("New Username:\n")
        val usernamenew1 = readLine()
        spark.sql(s"INSERT INTO Users VALUES('$firstname1','$usernamenew1','$passwordu1','ADMIN')")
        println("Username updated successfully")
        adminm()
      case 6 =>
        /*----UPDATE PASSWORD----*/
        println("Please verify your login info:\n" +
          "First Name:\n")
        val firstname2 = readLine()
        println("Username:\n")
        val usernameu2 = readLine()
        println("Password:\n")
        val passwordu2 = readLine()
        println("New Password:\n")
        val passwordnew2 = readLine()
        spark.sql(s"INSERT INTO Users VALUES('$firstname2','$usernameu2','$passwordnew2','ADMIN')")
        println("Password updated successfully")
        main(null)
      case 7 =>
        /*----LOGOUT----*/
        println("Successfully logged out")
        main(null)
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
        /*----QUERIES----*/
        queries()
        basicm()
      case 2 =>
        /*----UPDATE USERNAME----*/
        println("Please verify your login info:\n" +
          "First Name:\n")
        val firstname = readLine()
        println("Username:\n")
        val usernameu = readLine()
        println("Password:\n")
        val passwordu = readLine()
        println("New Username:\n")
        val usernamenew = readLine()
        spark.sql(s"INSERT INTO Users VALUES('$firstname','$usernamenew','$passwordu','BASIC')")
        println("Username updated successfully")
        basicm()
      case 3 =>
        /*----UPDATE PASSWORD----*/
        println("Please verify your login info:\n" +
          "First Name:\n")
        val firstname1 = readLine()
        println("Username:\n")
        val usernameu1 = readLine()
        println("Password:\n")
        val passwordu1 = readLine()
        println("New Password:\n")
        val passwordnew = readLine()
        spark.sql(s"INSERT INTO Users VALUES('$firstname1','$usernameu1','$passwordnew','BASIC')")
        println("Password updated successfully")
        main(null)
      case 4 =>
        /*----LOGOUT----*/
        println("Successfully logged out")
        main(null)
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
    spark.sql(s"INSERT INTO Users VALUES('$fn','$un','$pw','BASIC')")
    main(null)
  }

  /*----QUERIES----*/
  def queries(): Unit = {
    println(
      "[1] Average 40yd,Vertical,Bench,Broad Jump,3Cone,Shuttle\n" +
      "[2] Top 10 athletes in each event\n" +
      "[3] Min and max of each event\n" +
      "[4] Number of different schools that got drafted\n" +
      "[5] School with the most draft picks\n" +
      "[6] Clemson athlete draft picks\n")
    val admincc = readInt()
    admincc match{
      case 1 =>
        println(
          "[1] Overall average\n" +
          "[2] Position average\n")
        val average = readInt()
        average match {
          case 1 =>
            /*----Average 40yd,Vertical,Bench,Broad Jump,3Cone,Shuttle----*/
            spark.sql("SELECT AVG(40yd) AS 40yd,AVG(Vertical) AS Vertical,AVG(Bench) AS Bench,AVG(3Cone) AS 3Cone,AVG(Shuttle) AS Shuttle FROM 2019NFLCombine").show()
          case 2 =>
            /*----By position----*/
            spark.sql("SELECT Pos,AVG(40yd) AS 40yd,AVG(Vertical) AS Vertical,AVG(Bench) AS Bench,AVG(3Cone) AS 3Cone,AVG(Shuttle) AS Shuttle FROM 2019NFLCombine GROUP BY Pos").show()
        }
      case 2 =>
        /*----Top 10 athletes in each event----*/
        println(
          "[1] 40yd\n" +
          "[2] Vertical\n" +
          "[3] Bench\n" +
          "[4] 3Cone\n" +
          "[5] Shuttle")
        val top10 = readInt()
        top10 match{
          case 1 =>
            /*----40yd----*/
            spark.sql("SELECT Player,Pos,40yd FROM 2019NFLCombine ORDER BY 40yd ASC NULLS LAST LIMIT 10 ").show()
          case 2 =>
            /*----Vertical----*/
            spark.sql("SELECT Player,Pos,Vertical FROM 2019NFLCombine ORDER BY Vertical DESC NULLS LAST LIMIT 10 ").show()
          case 3 =>
            /*----Bench----*/
            spark.sql("SELECT Player,Pos,Bench FROM 2019NFLCombine ORDER BY Bench DESC NULLS LAST LIMIT 10 ").show()
          case 4 =>
            /*----3Cone----*/
            spark.sql("SELECT Player,Pos,3Cone FROM 2019NFLCombine ORDER BY 3Cone ASC NULLS LAST LIMIT 10 ").show()
          case 5 =>
            /*----Shuttle----*/
            spark.sql("SELECT Player,Pos,Shuttle FROM 2019NFLCombine ORDER BY Shuttle ASC NULLS LAST LIMIT 10 ").show()
        }
      case 3 =>
        /*----Min and max of each event----*/
        spark.sql("SELECT MIN(40yd) AS MIN_40,MAX(40yd) AS MAX_40 FROM 2019NFLCombine").show()
        spark.sql("SELECT MIN(Vertical) AS MIN_Vertical,MAX(Vertical) AS MAX_Vertical FROM 2019NFLCombine").show()
        spark.sql("SELECT MIN(Bench) AS MIN_Bench,MAX(Bench) AS MAX_Bench FROM 2019NFLCombine").show()
        spark.sql("SELECT MIN(3Cone) AS MIN_3Cone,MAX(3Cone) AS MAX_3Cone FROM 2019NFLCombine").show()
        spark.sql("SELECT MIN(Shuttle) AS MIN_Shuttle,MAX(Shuttle) AS MAX_Shuttle FROM 2019NFLCombine").show()
      case 4 =>
        /*----Number of different schools that got drafted----*/
        spark.sql("SELECT DISTINCT COUNT(School) AS Schools FROM 2019NFLCombine WHERE Drafted IS NOT NULL").show()
      case 5 =>
        /*----School with the most draft picks----*/
        spark.sql("SELECT School,COUNT(Drafted) AS Draft_Picks FROM 2019NFLCombine GROUP BY School ORDER BY Draft_Picks DESC NULLS LAST LIMIT 10").show()
      case 6 =>
        /*----Clemson athlete draft picks (tm/rnd/yr)----*/
        spark.sql("SELECT Player,School,Drafted AS Draft_Pick FROM 2019NFLCombine WHERE School='Clemson' AND Drafted IS NOT NULL").show(1000,1000,false)
    }
  }

  def main(args: Array[String]): Unit ={
    println(Console.RED+
      "\n███████████████████████████████████████████████████████████████████████████████████████" +
      "\n█▀▄▄▀█─▄▄─█▀░██░▄▄░███▄─▀█▄─▄█▄─▄▄─█▄─▄█████─▄▄▄─█─▄▄─█▄─▀█▀─▄█▄─▄─▀█▄─▄█▄─▀█▄─▄█▄─▄▄─█" +
      "\n██▀▄██─██─██░██▄▄▄░████─█▄▀─███─▄████─██▀███─███▀█─██─██─█▄█─███─▄─▀██─███─█▄▀─███─▄█▀█" +
      "\n▀▄▄▄▄▀▄▄▄▄▀▄▄▄▀▄▄▄▄▀▀▀▄▄▄▀▀▄▄▀▄▄▄▀▀▀▄▄▄▄▄▀▀▀▄▄▄▄▄▀▄▄▄▄▀▄▄▄▀▄▄▄▀▄▄▄▄▀▀▄▄▄▀▄▄▄▀▀▄▄▀▄▄▄▄▄▀")
    println(Console.RESET)
    println(Console.BLUE)
    println("Login or create an account to view an analysis of the 2019 NFL Combine")
    println(Console.RESET)
    println("[1] Login \n" +
      "[2] Create an account")
    val user = readInt()
    user match{
      case 1 =>
        /*----LOGIN----*/
        try {
          println("Enter username:\n")
          val username = readLine()
          println("Enter password:\n")
          val password = readLine()
          val login = spark.sql(s"SELECT Permission FROM Users WHERE Username='$username' AND Password='$password'")
          if (login.head().getString(0) == "ADMIN") {
            val welcome = spark.sql(s"SELECT Name FROM Users WHERE Username='$username' AND Password='$password'").head().getString(0)
            println(s"Welcome back, $welcome\n")
            adminm()
          } else {
            val welcome = spark.sql(s"SELECT Name FROM Users WHERE Username='$username' AND Password='$password'").head().getString(0)
            println(s"Welcome back, $welcome\n")
            basicm()
          }
        } catch{
          case e: java.util.NoSuchElementException => println("Username or Password is incorrect")
            main(null)
        }
      case 2 =>
        /*----NEW USER----*/
        newusr(2)
    }
  }

}

/*----QUERIES----*/
  /*----Average 40yd,Vertical,Bench,Broad Jump,3Cone,Shuttle----*/
  //spark.sql("SELECT AVG(40yd) AS 40yd,AVG(Vertical) AS Vertical,AVG(Bench) AS Bench,AVG(3Cone) AS 3Cone,AVG(Shuttle) AS Shuttle FROM 2019NFLCombine").show()
    /*----By position----*/
    //spark.sql("SELECT Pos,AVG(40yd) AS 40yd,AVG(Vertical) AS Vertical,AVG(Bench) AS Bench,AVG(3Cone) AS 3Cone,AVG(Shuttle) AS Shuttle FROM 2019NFLCombine GROUP BY Pos").show()

  /*----Top 10 athletes in each event----*/
  //spark.sql("SELECT Player,Pos,40yd FROM 2019NFLCombine ORDER BY 40yd ASC NULLS LAST LIMIT 10 ").show()
  //spark.sql("SELECT Player,Pos,Vertical FROM 2019NFLCombine ORDER BY Vertical DESC NULLS LAST LIMIT 10 ").show()
  //spark.sql("SELECT Player,Pos,Bench FROM 2019NFLCombine ORDER BY Bench DESC NULLS LAST LIMIT 10 ").show()
  //spark.sql("SELECT Player,Pos,3Cone FROM 2019NFLCombine ORDER BY 3Cone ASC NULLS LAST LIMIT 10 ").show()
  //spark.sql("SELECT Player,Pos,Shuttle FROM 2019NFLCombine ORDER BY Shuttle ASC NULLS LAST LIMIT 10 ").show()

  /*----Min and max of each event----*/
  //spark.sql("SELECT MIN(40yd) AS MIN_40,MAX(40yd) AS MAX_40 FROM 2019NFLCombine").show()
  //spark.sql("SELECT MIN(Vertical) AS MIN_Vertical,MAX(Vertical) AS MAX_Vertical FROM 2019NFLCombine").show()
  //spark.sql("SELECT MIN(Bench) AS MIN_Bench,MAX(Bench) AS MAX_Bench FROM 2019NFLCombine").show()
  //spark.sql("SELECT MIN(3Cone) AS MIN_3Cone,MAX(3Cone) AS MAX_3Cone FROM 2019NFLCombine").show()
  //spark.sql("SELECT MIN(Shuttle) AS MIN_Shuttle,MAX(Shuttle) AS MAX_Shuttle FROM 2019NFLCombine").show()

  /*----Number of different schools that got drafted----*/
  //spark.sql("SELECT DISTINCT COUNT(School) AS Schools FROM 2019NFLCombine WHERE Drafted IS NOT NULL").show()

  /*----School with the most draft picks----*/
  //spark.sql("SELECT School,COUNT(Drafted) AS Draft_Picks FROM 2019NFLCombine GROUP BY School ORDER BY Draft_Picks DESC NULLS LAST LIMIT 10").show()

  /*----Clemson athlete draft picks (tm/rnd/yr)----*/
  //spark.sql("SELECT Player,School,Drafted AS Draft_Pick FROM 2019NFLCombine WHERE School='Clemson' AND Drafted IS NOT NULL").show(1000,1000,false)

