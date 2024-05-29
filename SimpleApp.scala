import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{BooleanType, DateType, TimestampType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, unix_timestamp, from_unixtime, to_date}
import org.apache.spark.sql.functions.{col, count, avg, monotonically_increasing_id, unix_timestamp, to_date}


//java -jar metabase.jar
//export PATH=/usr/gide/jdk-1.8/bin:$PATH
//export JAVA_HOME=/usr/gide/jdk-1.8
//export PATH=/usr/gide/sbt-1.3.13/bin:$PATH
//sbt clean compile
//sbt run --mem 20000
//

object SimpleApp {
  def main(args: Array[String]): Unit = {


    // Initialisation de Spark
    val spark = SparkSession.builder.appName("ETL")
    .master("local[4]")
    .config("spark.executor.memory", "100g")
    .config("spark.driver.memory", "100g")   
    .getOrCreate()

    // Lecture du fichier business.json
      val businessFile10 = "data/yelp_academic_dataset_business.json"

      //	Dataframe Business (business_id, business_name, category)
      var file_business10 = spark.read.json(businessFile10).cache()

      file_business10 = file_business10
        .withColumn("categories", explode(org.apache.spark.sql.functions.split(col("categories"), ",")))
      file_business10 = file_business10
        .withColumnRenamed("categories", "category_name")
      file_business10 = file_business10
        .filter(col("category_name").notEqual("None"))
      file_business10 = file_business10
        .drop(col("categories"))
      file_business10 = file_business10
        .dropDuplicates()
      file_business10 = file_business10
        .na.drop()  
      file_business10.printSchema()



      var dim_category = file_business10.select("business_id","category_name")
      //Supression des doublons
      dim_category = dim_category.dropDuplicates("business_id")
      //Identifiant category_id, id qui va incrémenter automatiquement
      dim_category = dim_category
        .withColumn("category_id", monotonicallyIncreasingId)    
      dim_category.printSchema()



      //Dataframe Hours (hours_id, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday)
      var dim_hours = file_business10
        .select("business_id",
        "hours.Monday",
        "hours.Tuesday",
        "hours.Wednesday",
        "hours.Thursday",
        "hours.Friday",
        "hours.Saturday",
        "hours.Sunday")
      //Supression des doublons
      dim_hours = dim_hours.dropDuplicates()
      //Identifiant parking_id, id qui va incrémenter automatiquement
      dim_hours = dim_hours
        .withColumn("hours_id", monotonicallyIncreasingId)
      //dim_hours = dim_hours
        //.withColumnRenamed("business_id", "fk_business_id")
      dim_hours.printSchema()


      //Dataframe Location (location_id, address, city, state, postal_code, longitude, latitude)
      var dim_location = file_business10
        .select("business_id",
        "address",
        "city",
        "state",
        "postal_code",
        "longitude",
        "latitude")
      //Supression des doublons
      dim_location = dim_location.dropDuplicates("business_id")
      //Identifiant parking_id, id qui va incrémenter automatiquement
      dim_location = dim_location
        .withColumn("location_id", monotonicallyIncreasingId)
      dim_location.printSchema()


    //notation dataframe

    var dim_rating = file_business10
    .select("business_id",
        "name",
        "stars",
        "review_count")
    dim_rating = dim_rating
     .withColumn("rating_id", monotonicallyIncreasingId)
    dim_rating.printSchema()


    /*-------------------------//dataframe business--------------------------------------------*/


// Lecture du fichier checkin.json
    var dim_checkin = spark.read
      .json("data/yelp_academic_dataset_checkin.json")
    dim_checkin = dim_checkin
      .withColumn("business_id", substring(col("business_id"), 3, 99))
    // On éclate le champ date, on compte le nombre de virgules car entre chaque virgule il y a une date
    dim_checkin = dim_checkin
      .withColumn("total_checkin", size(split(col("date"), ",")))
    //dim_checkin = dim_checkin
      //.withColumnRenamed("business_id", "fk_business_id")
    dim_checkin = dim_checkin
     .withColumn("checkin_id", monotonicallyIncreasingId)
    dim_checkin = dim_checkin
     .withColumn("date", substring(col("date"), 1, 4000))
    dim_checkin = dim_checkin.dropDuplicates("business_id")
    dim_checkin = dim_checkin
      .limit(100000)


/*--------------------------------------------------------------------------------------------------------------*/

// Lecture du fichier tip.csv
    var dim_tip = spark.read
      .option("header",true)
      .csv("data/yelp_academic_dataset_tip.csv")
      .select("business_id","user_id","compliment_count","date","text")
    // Cast de la colonne date en timestamp
    dim_tip = dim_tip
      .withColumn("date", col("date").cast(TimestampType))
      // Exemple d'ajout d'une colonne fictive (hypothétique)
    dim_tip = dim_tip
    .withColumn("user_id", col("user_id"))

    // Dataframe Tip (tip_id, text, date, compliment_count, business_id)
    dim_tip = dim_tip
      .select("business_id","user_id","text","date","compliment_count")

    dim_tip = dim_tip.dropDuplicates(Seq("business_id", "user_id"))
    // Affichage du dataframe Tip
    dim_tip.printSchema()



/*----------------------------------------------chargement fichier calendrier ----------------------------------------------------------------*/

// Lecture du fichier calendrier.csv
    var dim_temps = spark.read
      .option("header",true)
      .csv("data/calendrier.csv")
      .select("IS_WORKDAY","IS_HOLIDAY","IS_HOLIDAY_LEAVE","A_DATE","DAY_FULL","DAY_FULL_CAPITAL_CASE","DAY_FULL_LOWER_CASE","DAY_ABBREV",
"IS_WEEKDAY","IS_WEEKDAY_BY_REGEX","DAY_OF_WEEK","DAY_OF_MONTH","DAY_OF_YEAR","WEEK_OF_MONTH","MONTH_FULL","MONTH_ABBREV","MONTH_NUMBER","QUARTER",
"YEAR_SHORT","YEAR_FULL","FISCAL_QUARTER","FISCAL_YEAR","FISCAL_YEAR_FULL","TOMORROW","ONE_WEEK_LATER","ONE_MONTH_LATER","THREE_MONTHS_LATER","ONE_YEAR_LATER")

      // Exemple d'ajout d'une colonne fictive (hypothétique)
    //dim_tip = dim_tip
    //.withColumn("user_id", col("user_id"))
    dim_temps = dim_temps
      .select("IS_WORKDAY","IS_HOLIDAY","IS_HOLIDAY_LEAVE","A_DATE","DAY_FULL","DAY_FULL_CAPITAL_CASE","DAY_FULL_LOWER_CASE","DAY_ABBREV",
"IS_WEEKDAY","IS_WEEKDAY_BY_REGEX","DAY_OF_WEEK","DAY_OF_MONTH","DAY_OF_YEAR","WEEK_OF_MONTH","MONTH_FULL","MONTH_ABBREV","MONTH_NUMBER","QUARTER",
"YEAR_SHORT","YEAR_FULL","FISCAL_QUARTER","FISCAL_YEAR","FISCAL_YEAR_FULL","TOMORROW","ONE_WEEK_LATER","ONE_MONTH_LATER","THREE_MONTHS_LATER","ONE_YEAR_LATER")
    
    dim_temps = dim_temps.dropDuplicates()
    // Affichage du dataframe Tip
    dim_temps.printSchema()

    /*------------------------------------les connexions------------------------------------*/

//paramètres de la connexion BD
import java.util.Properties
     //BD Oracle
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val urlOracle = "jdbc:oracle:thin:@stendhal:1521:enss2023"
    val connectionPropertiesOracle = new Properties()
    connectionPropertiesOracle.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    connectionPropertiesOracle.setProperty("user", "io366020")
    connectionPropertiesOracle.setProperty("password", "io366020")


    // BD POSTGRESQL
    Class.forName("org.postgresql.Driver")
    val urlPostgreSQL = "jdbc:postgresql://stendhal.iem:5432/tpid2020"
    import java.util.Properties
    val connectionPropertiesPostgreSQL = new Properties()
    connectionPropertiesPostgreSQL.setProperty("driver", "org.postgresql.Driver")
    connectionPropertiesPostgreSQL.setProperty("user", "tpid")
    connectionPropertiesPostgreSQL.setProperty("password", "tpid")


/*----------------------------------------------------------------------------------------------------------------------------------------*/


    // Lecture et affichage de la table user de PostgreSQL
    var dim_user = spark.read
      .option("numPartitions", 10)
      .option("partitionColumn", "yelping_since")
      .option("lowerBound", "2010-01-01")
      .option("upperBound", "2019-12-31")
      .jdbc(urlPostgreSQL, "yelp.user", connectionPropertiesPostgreSQL)
     dim_user = dim_user.dropDuplicates("user_id")
      dim_user = dim_user
      .limit(100000)


    var fact_user = spark.read
      .option("numPartitions", 10)
      .option("partitionColumn", "yelping_since")
      .option("lowerBound", "2010-01-01")
      .option("upperBound", "2019-12-31")
      .jdbc(urlPostgreSQL, "yelp.user", connectionPropertiesPostgreSQL)
       fact_user = fact_user
       .withColumn("user_rating_id",monotonicallyIncreasingId)
    fact_user = fact_user.dropDuplicates("user_id")



    // Lecture et affichage de la table friend de PostgreSQL
    var dim_friend = spark.read
      .option("dbtable", "(select user_id,friend_id,spark_partition from yelp.friend) as subquery")
      .option("user", "tpid")
      .option("password", "tpid")
      .jdbc(urlPostgreSQL, "yelp.friend", connectionPropertiesPostgreSQL)

    dim_friend = dim_friend
      .limit(100000)
    dim_friend = dim_friend
       .withColumn("user_id", col("user_id"))
    dim_friend = dim_friend
       .dropDuplicates()
    dim_friend = dim_friend.dropDuplicates(Seq("user_id", "friend_id"))

    // Creation de la dataframe Elite    Lecture et affichage de la table elite de PostgreSQL
    var dim_elite = spark.read
      .option("user", "tpid")
      .option("password", "tpid")
      .jdbc(urlPostgreSQL, "yelp.elite", connectionPropertiesPostgreSQL)
     dim_elite = dim_elite.dropDuplicates(Seq("user_id", "year"))
      dim_elite = dim_elite
      .limit(100000)

     //lecture et affichage de la table review de postgresSQL
       //Dataframe review (review_id,business_id,user_id,date,text,funny,stars)

      var dim_review = spark.read
      .format("jdbc")
      .option("url",urlPostgreSQL)
      .option("user", "tpid")
      .option("password", "tpid")
      .option("dbtable","(select review_id,business_id,user_id,date,text,funny,stars from yelp.review) as subq")
      .option("partitionColumn", "date")
      .option("lowerBound", "2010-01-01")
      .option("upperBound", "2019-12-31")
      .option("numPartitions", 40)
      .load()
    dim_review = dim_review
      .limit(100000)
    dim_review = dim_review.dropDuplicates("review_id")
    dim_review = dim_review
        .withColumn("text", substring(col("text"), 0, 255))

    dim_review = dim_review
     .withColumn("a_date",to_date(unix_timestamp(col("date"), "dd-MMM-yyyy").cast("timestamp"), "yyyy-MM-dd"))
    


// On crée des tables temporaires à partir de nos dataframe pour pouvoir faire des requêtes sql dessus.
   
    dim_rating.createOrReplaceTempView("dim_rating")
    dim_hours.createOrReplaceTempView("dim_hours")
    dim_category.createOrReplaceTempView("dim_category")
    dim_location.createOrReplaceTempView("dim_location")
    file_business10.createOrReplaceTempView("business10")
    dim_checkin.createOrReplaceTempView("dim_checkin")
    dim_temps.createOrReplaceTempView("dim_temps")
   
    
//Table temporaires de User et Friend

      dim_user.createOrReplaceTempView("dim_user")
      fact_user.createOrReplaceTempView("fact_user")
      dim_friend.createOrReplaceTempView("dim_friend")
      dim_elite.createOrReplaceTempView("dim_elite")
      dim_review.createOrReplaceTempView("dim_review")


//Table de dimensions avec les bon formats et les bons attributs


      dim_temps = dim_temps
        .select("IS_WORKDAY","IS_HOLIDAY","IS_HOLIDAY_LEAVE","a_date","DAY_FULL","DAY_FULL_CAPITAL_CASE","DAY_FULL_LOWER_CASE","DAY_ABBREV",
              "IS_WEEKDAY","IS_WEEKDAY_BY_REGEX","DAY_OF_WEEK","DAY_OF_MONTH","DAY_OF_YEAR","WEEK_OF_MONTH","MONTH_FULL","MONTH_ABBREV","MONTH_NUMBER","QUARTER",
              "YEAR_SHORT","YEAR_FULL","FISCAL_QUARTER","FISCAL_YEAR","FISCAL_YEAR_FULL","TOMORROW","ONE_WEEK_LATER","ONE_MONTH_LATER","THREE_MONTHS_LATER","ONE_YEAR_LATER")
      dim_location = dim_location
        .select("location_id","business_id","address","city","state","postal_code")
      dim_rating = dim_rating
        .select("rating_id","business_id","name","stars","review_count")
      dim_hours = dim_hours
        .select("hours_id","Business_id","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")
      dim_category = dim_category
        .select("category_id","business_id","category_name")
      dim_checkin = dim_checkin
        .select("checkin_id","business_id","total_checkin","date")
      dim_tip = dim_tip
        .select("business_id","user_id","compliment_count","text","date")

      dim_friend = dim_friend
        .select("friend_id","user_id")
      dim_elite = dim_elite
        .select("user_id","year")
      fact_user = fact_user
        .select("user_id","user_rating_id","name","average_stars","review_count","yelping_since")
      dim_user = dim_user
        .select("user_id","cool","fans","funny","compliment_note","name","average_stars","review_count","yelping_since")

      dim_review = dim_review
        .select("user_id","business_id","review_id","date","a_date","text","stars")



      //Affichage des tables de faits et de dimensions
      println("dim_location")
      dim_location.printSchema
      println("dim_rating")
      dim_rating.printSchema
      println("dim_hours")
      dim_hours.printSchema
      println("dim_category")
      dim_category.printSchema
      println("dim_checkin")
      dim_checkin.printSchema
      println("dim_tip")
      dim_tip.printSchema

      println("dim_temps")
      dim_temps.printSchema

     
      println("dim_elite")
      dim_elite.printSchema
      println("fact_user")
      fact_user.printSchema

      println("dim_friend")
      dim_friend.printSchema
      println("dim_user")
      dim_user.printSchema
      println("dim_review")
      dim_review.printSchema


import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
     val dialect = new OracleDialect
     JdbcDialects.registerDialect(dialect)

     print("---------------------------------------------------------------------------------------------------------------------writing to DATABASE---------------------------------------------------------------------------------------------------------------------")


    
     dim_rating
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_rating", connectionPropertiesOracle)
  
     dim_category
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_category", connectionPropertiesOracle)

     dim_hours
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_hours", connectionPropertiesOracle)
    
     dim_location
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_location", connectionPropertiesOracle)


     dim_checkin
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_checkin", connectionPropertiesOracle)

     dim_tip
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_tip", connectionPropertiesOracle)

     dim_temps
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_temps", connectionPropertiesOracle)


     dim_friend
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_friend", connectionPropertiesOracle)

     dim_elite
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_elite", connectionPropertiesOracle)

     fact_user
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "fact_user", connectionPropertiesOracle)

      dim_review
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_review", connectionPropertiesOracle)
       
     dim_user
       .write
       .mode(SaveMode.Overwrite)
       .jdbc(urlOracle, "dim_user", connectionPropertiesOracle)
 
/*------------------------------------------------a revoir-------------------------------------------

*/
val reviewCounts = dim_review.groupBy("user_id")
  .agg(countDistinct("review_id").alias("new_review_count"))

val tipCounts = dim_tip.groupBy("user_id")
  .agg(count("text").alias("total_tips"))



val friendCounts = dim_friend.groupBy("user_id")
  .agg(countDistinct("friend_id").alias("friend_count"))

val eliteYearsCounts = dim_elite.groupBy("user_id")
  .agg(countDistinct("year").alias("elite_years_count"))

// Jointure de fact_user avec les agrégats et dim_user
val enriched_fact_user = fact_user
  .join(reviewCounts, "user_id")
  .join(tipCounts, "user_id")
  // Suppression de la jointure avec checkinCounts
  .join(friendCounts, "user_id")
  .join(eliteYearsCounts, "user_id")
  .join(dim_user, fact_user("user_id") === dim_user("user_id"), "left") // Jointure avec dim_user
  .select(
    fact_user("user_id"),
    fact_user("name"),
    fact_user("average_stars"),
    fact_user("review_count").alias("existing_review_count"),
    fact_user("yelping_since"),
    reviewCounts("new_review_count"),
    // checkinCounts("total_checkins"), // Supprimé car "user_id" n'existe pas dans dim_checkin
    friendCounts("friend_count"),
    eliteYearsCounts("elite_years_count"),
    tipCounts("total_tips")
  ).distinct()

// Créer une vue temporaire pour vérification
enriched_fact_user.createOrReplaceTempView("enriched_fact_user_view")

// Affichage de la vue temporaire pour vérification
spark.sql("SELECT * FROM enriched_fact_user_view").show()

// Charger les données dans une table persistante
// Remplacez yourDatabaseName.yourTableName par le nom de votre base de données et de votre table
enriched_fact_user
  .limit(100000)
  .write
  .mode(SaveMode.Overwrite) // Utilisez Append pour ajouter des données ou Overwrite pour remplacer
  .jdbc(urlOracle, "fact_user", connectionPropertiesOracle)

var fact_business = dim_rating
      .alias("r")
      .join(dim_category.alias("c"), col("r.business_id").equalTo(col("c.business_id")), "left_outer")
      .join(dim_tip.alias("t"), col("r.business_id").equalTo(col("t.business_id")), "left_outer")
      .join(dim_location.alias("l"), col("r.business_id").equalTo(col("l.business_id")), "left_outer")
      .join(dim_review.alias("rv"), col("r.business_id").equalTo(col("rv.business_id")), "left_outer")
      .join(dim_checkin.alias("ck"), col("r.business_id").equalTo(col("ck.business_id")), "left_outer")
      .join(dim_hours.alias("h"), col("r.business_id").equalTo(col("h.business_id")), "left_outer")
      .join(dim_temps.alias("cal"), col("rv.a_date").equalTo(col("cal.a_date")), "left_outer")
      .select(
        col("r.business_id"),
        col("r.rating_id").alias("id_rating"),
        col("c.category_id").alias("id_category"),
        col("l.location_id"),
        col("ck.business_id").alias("business_id_checkin"),
        col("ck.total_checkin"),
        col("rv.a_date"),
        count(col("rv.review_id")).over(Window.partitionBy(col("r.business_id"))).alias("total_reviews"),
        avg(col("rv.stars")).over(Window.partitionBy(col("r.business_id"))).alias("average_review_stars"),
        count(col("t.text")).over(Window.partitionBy(col("r.business_id"))).alias("total_tips"),
        avg(col("r.stars")).over(Window.partitionBy(col("r.business_id"))).alias("average_stars"),
        col("h.hours_id")
      )
      .distinct()
      .withColumn("id_fact_business", monotonically_increasing_id())

println("fact_business")
fact_business.printSchema

fact_business.createOrReplaceTempView("fact_business")

fact_business
      .limit(100000)
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(urlOracle, "fact_business", connectionPropertiesOracle)

fact_business.show(10)

/*--------------------------------------------FIN-------------------------------------------*/

/*-----------------------------------group by , rollup-, cube------------------------------------------------*/

val reviewsPerCategory = dim_category
  .join(dim_review, dim_category("business_id") === dim_review("business_id"))
  .groupBy("category_name")
  .agg(count("review_id").alias("total_reviews"))
  .orderBy(desc("total_reviews"))

reviewsPerCategory.show(5)


val reviewsByDate = spark.sql("""
SELECT year, month, COUNT(review_id) AS total_reviews
FROM (
  SELECT *, YEAR(date) AS year, MONTH(date) AS month FROM dim_review
)
GROUP BY ROLLUP(year, month)
ORDER BY year, month
""")

reviewsByDate.show(6)


/*----------------------------data cube----------------------*/

/*
val businessAnalysis = fact_business
  .join(dim_category, "business_id")
  .join(dim_temps, "a_date")
  .groupBy("year_full", "month_number", "category_name")
  .agg(
    count("review_id").alias("total_reviews"),
    avg("stars").alias("average_stars"),
    count("tip_id").alias("total_tips"),
    count("checkin_id").alias("total_checkins")
  )

  businessAnalysis.createOrReplaceTempView("business_analysis_view")


  businessAnalysis.show(10)

*/

/*---------------------------------------------------------------------------*/


// DataFrame: file_business10
println("DataFrame: file_business10")
file_business10.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_category
println("DataFrame: dim_category")
dim_category.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_hours
println("DataFrame: dim_hours")
dim_hours.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_location
println("DataFrame: dim_location")
dim_location.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_rating
println("DataFrame: dim_rating")
dim_rating.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_checkin
println("DataFrame: dim_checkin")
dim_checkin.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_tip
println("DataFrame: dim_tip")
dim_tip.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_temps
println("DataFrame: dim_temps")
dim_temps.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_user
println("DataFrame: dim_user")
dim_user.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: fact_user
println("DataFrame: fact_user")
fact_user.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_friend
println("DataFrame: dim_friend")
dim_friend.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_elite
println("DataFrame: dim_elite")
dim_elite.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: dim_review
println("DataFrame: dim_review")
dim_review.show(10)

// Séparateur
println("------------------------------------------------")

// DataFrame: fact_business 
println("DataFrame: fact_business")
fact_business.show(10)


 
      print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- end wrinting to DATABASE---------------------------------------------------------------------------------------------------------------------")
	
     spark.stop()
   }
   import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
   import org.apache.spark.sql.types._
   class OracleDialect extends JdbcDialect {
     override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
       case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.INTEGER))
       case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
       case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
       case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
       case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
       case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
       case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
       case StringType => Some(JdbcType("VARCHAR2(4000)", java.sql.Types.VARCHAR))
       case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))

       //Ligne ajoutée pour timestamp
       case TimestampType => Some(JdbcType("TIMESTAMP",java.sql.Types.TIMESTAMP))
       case _ => None
     }
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")
   }
}