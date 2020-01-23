import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.spark.rdd.RDD
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicSessionCredentials
import java.io.File
//Cassandra
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._


object Query2 {

  def main(args: Array[String]): Unit = {

    // Des réglages optionnels du job spark. Les réglages par défaut fonctionnent très bien pour ce TP.
    // On vous donne un exemple de setting quand même
    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12"
    ))
      .set("spark.cassandra.connection.host", "192.168.123.10")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")


    // Initialisation du SparkSession qui est le point d'entrée vers Spark SQL (donne accès aux dataframes, aux RDD,
    // création de tables temporaires, etc., et donc aux mécanismes de distribution des calculs)
    val spark = SparkSession
      .builder
      .config(conf)
      .appName("Projet Gdelt : Query 2")
      .getOrCreate()

    val sc = new SparkContext(conf)

    import spark.implicits._

    val AWS_ID = "TODO"
    val AWS_KEY = "TODO"
    val AWS_TOKEN = "TODO"
    val s3_name = "TODO"

    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    sc.hadoopConfiguration.set("fs.s3a.access.key", AWS_ID) // mettre votre ID du fichier credentials.csv
    sc.hadoopConfiguration.set("fs.s3a.secret.key", AWS_KEY) // mettre votre secret du fichier credentials.csv
    sc.hadoopConfiguration.set("fs.s3a.session.token", AWS_TOKEN)

    /** **************************************
     * Charger un fichier csv dans un rdd depuis s3
     * ********************************************/

    val textRDDEvents: RDD[String] = sc.binaryFiles("s3://" + s3_name + "/201901*.export.CSV.zip").
      flatMap { // decompresser les fichiers
        case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
            takeWhile{ case null => zis.close(); false
            case _ => true }.
            flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }
      }

    // *** Mentions ***
    val textRDDMentions: RDD[String] = sc.binaryFiles("s3://" + s3_name + "/201901*.mentions.CSV.zip").
      flatMap { // decompresser les fichiers
        case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
            takeWhile{ case null => zis.close(); false
            case _ => true }.
            flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }
      }

    /** ************************************************************
     * Ajout des informations colonnes et creation d'un Dataframe
     * ************************************************************* */

    // EVENTS

    val dfEvents: DataFrame = textRDDEvents.toDF.withColumn("GLOBALEVENTID", split($"value", "\\t").getItem(0))
      .withColumn("Day", split($"value", "\\t").getItem(1))
      .withColumn("MonthYear", split($"value", "\\t").getItem(2))
      .withColumn("Year", split($"value", "\\t").getItem(3))
      .withColumn("FractionDate", split($"value", "\\t").getItem(4))
      .withColumn("Actor1Code", split($"value", "\\t").getItem(5))
      .withColumn("Actor1Name", split($"value", "\\t").getItem(6))
      .withColumn("Actor1CountryCode", split($"value", "\\t").getItem(7))
      .withColumn("Actor1KnownGroupCode", split($"value", "\\t").getItem(8))
      .withColumn("Actor1EthnicCode", split($"value", "\\t").getItem(9))
      .withColumn("Actor1Religion1Code", split($"value", "\\t").getItem(10))
      .withColumn("Actor1Religion2Code", split($"value", "\\t").getItem(11))
      .withColumn("Actor1Type1Code", split($"value", "\\t").getItem(12))
      .withColumn("Actor1Type2Code", split($"value", "\\t").getItem(13))
      .withColumn("Actor1Type3Code", split($"value", "\\t").getItem(14))
      .withColumn("Actor2Code", split($"value", "\\t").getItem(15))
      .withColumn("Actor2Name", split($"value", "\\t").getItem(16))
      .withColumn("Actor2CountryCode", split($"value", "\\t").getItem(17))
      .withColumn("Actor2KnownGroupCode", split($"value", "\\t").getItem(18))
      .withColumn("Actor2EthnicCode", split($"value", "\\t").getItem(19))
      .withColumn("Actor2Religion1Code", split($"value", "\\t").getItem(20))
      .withColumn("Actor2Religion2Code", split($"value", "\\t").getItem(21))
      .withColumn("Actor2Type1Code", split($"value", "\\t").getItem(22))
      .withColumn("Actor2Type2Code", split($"value", "\\t").getItem(23))
      .withColumn("Actor2Type3Code", split($"value", "\\t").getItem(24))
      .withColumn("IsRootEvent", split($"value", "\\t").getItem(25))
      .withColumn("EventCode", split($"value", "\\t").getItem(26))
      .withColumn("EventBaseCode", split($"value", "\\t").getItem(27))
      .withColumn("EventRootCode", split($"value", "\\t").getItem(28))
      .withColumn("QuadClass", split($"value", "\\t").getItem(29))
      .withColumn("GoldsteinScale", split($"value", "\\t").getItem(30))
      .withColumn("NumMentions", split($"value", "\\t").getItem(31))
      .withColumn("NumSources", split($"value", "\\t").getItem(32))
      .withColumn("NumArticles", split($"value", "\\t").getItem(33))
      .withColumn("AvgTone", split($"value", "\\t").getItem(34))
      .withColumn("Actor1Geo_Type", split($"value", "\\t").getItem(35))
      .withColumn("Actor1Geo_FullName", split($"value", "\\t").getItem(36))
      .withColumn("Actor1Geo_CountryCode", split($"value", "\\t").getItem(37))
      .withColumn("Actor1Geo_ADM1Code", split($"value", "\\t").getItem(38))
      .withColumn("Actor1Geo_ADM2Code", split($"value", "\\t").getItem(39))
      .withColumn("Actor1Geo_Lat", split($"value", "\\t").getItem(40))
      .withColumn("Actor1Geo_Long", split($"value", "\\t").getItem(41))
      .withColumn("Actor1Geo_FeatureID", split($"value", "\\t").getItem(42))
      .withColumn("Actor2Geo_Type", split($"value", "\\t").getItem(43))
      .withColumn("Actor2Geo_FullName", split($"value", "\\t").getItem(44))
      .withColumn("Actor2Geo_CountryCode", split($"value", "\\t").getItem(45))
      .withColumn("Actor2Geo_ADM1Code", split($"value", "\\t").getItem(46))
      .withColumn("Actor2Geo_ADM2Code", split($"value", "\\t").getItem(47))
      .withColumn("Actor2Geo_Lat", split($"value", "\\t").getItem(48))
      .withColumn("Actor2Geo_Long", split($"value", "\\t").getItem(49))
      .withColumn("Actor2Geo_FeatureID", split($"value", "\\t").getItem(50))
      .withColumn("ActionGeo_Type", split($"value", "\\t").getItem(51))
      .withColumn("ActionGeo_FullName", split($"value", "\\t").getItem(52))
      .withColumn("ActionGeo_CountryCode", split($"value", "\\t").getItem(53))
      .withColumn("ActionGeo_ADM1Code", split($"value", "\\t").getItem(54))
      .withColumn("ActionGeo_ADM2Code", split($"value", "\\t").getItem(55))
      .withColumn("ActionGeo_Lat", split($"value", "\\t").getItem(56))
      .withColumn("ActionGeo_Long", split($"value", "\\t").getItem(57))
      .withColumn("ActionGeo_FeatureID", split($"value", "\\t").getItem(58))
      .withColumn("DATEADDED", split($"value", "\\t").getItem(59))
      .withColumn("SOURCEURL", split($"value", "\\t").getItem(60))
      .drop("value")

    // MENTIONS
    val dfMentions: DataFrame = textRDDMentions.toDF.withColumn("GLOBALEVENTID", split($"value", "\\t").getItem(0))
      .withColumn("EventTimeDate", split($"value", "\\t").getItem(1))
      .withColumn("MentionTimeDate", split($"value", "\\t").getItem(2))
      .withColumn("MentionType", split($"value", "\\t").getItem(3))
      .withColumn("MentionSourceName", split($"value", "\\t").getItem(4))
      .withColumn("MentionIdentifier", split($"value", "\\t").getItem(5))
      .withColumn("SentenceID", split($"value", "\\t").getItem(6))
      .withColumn("Actor1CharOffset", split($"value", "\\t").getItem(7))
      .withColumn("Actor2CharOffset", split($"value", "\\t").getItem(8))
      .withColumn("ActionCharOffset", split($"value", "\\t").getItem(9))
      .withColumn("InRawText", split($"value", "\\t").getItem(10))
      .withColumn("Confidence", split($"value", "\\t").getItem(11))
      .withColumn("MentionDocLen", split($"value", "\\t").getItem(12))
      .withColumn("MentionDocTone", split($"value", "\\t").getItem(13))
      .withColumn("MentionDocTranslationInfo", split($"value", "\\t").getItem(14))
      .withColumn("Extras", split($"value", "\\t").getItem(15))
      .drop("value")



    val df_country_events = dfEvents.select("GLOBALEVENTID","ActionGeo_CountryCode", "Day")
      .filter(!($"ActionGeo_CountryCode".isNaN || $"ActionGeo_CountryCode".isNull || $"ActionGeo_CountryCode" === ""))
      .join(
        dfMentions.select("MentionIdentifier","GLOBALEVENTID" ), "GLOBALEVENTID")
      .withColumn("year", substring($"Day", 0, 4))
      .withColumn("month", substring($"Day", 5, 2))
      .withColumn("day", substring($"Day", 7, 2))
      .groupBy("ActionGeo_CountryCode","GLOBALEVENTID","year", "month", "day")
      .agg(count($"MentionIdentifier").alias("num_mentions"))
      .orderBy($"num_mentions".desc)
      .withColumnRenamed("ActionGeo_CountryCode","country")
      .withColumnRenamed("GLOBALEVENTID","event")

    /***********************************
     * Save Calculation in S3
     ******************************/

    @transient val awsClient = new AmazonS3Client(new BasicSessionCredentials(AWS_ID, AWS_KEY, AWS_TOKEN) )


    /***
    df_country_events
      .write
      .mode(SaveMode.Overwrite)
      .parquet("s3://" + s3_name + "/dfPays_Events.parquet/")
     ***/


    /***********************************************************************************************
     ************************** Import to Cassandra *********************************************
     ******************************************************************************************/
    /**************************************
     * Creation of the KEYSPACE and Table
     **************************************/
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(
        """
           CREATE KEYSPACE IF NOT EXISTS gdelt
           WITH REPLICATION =
           {'class': 'SimpleStrategy', 'replication_factor': 2 };
        """)
      session.execute(
        """
           CREATE TABLE IF NOT EXISTS gdelt.country_events (
              country text,
              year int,
              month int,
              day int,
              event int,
              num_mentions int,
              PRIMARY KEY (country, year, month, day, event, num_mentions)
            );
        """
      )
    }

    /*************************
     * Import of the data
     *************************/
    df_country_events.write
      .cassandraFormat("country_events", "gdelt")
      .save()

  }
}
