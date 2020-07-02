import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.rdd.RDD
//Cassandra
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._


object Query3 {

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
      .set("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
      .set("spark.cassandra.input.consistency.level", "LOCAL_ONE")


    // Initialisation du SparkSession qui est le point d'entrée vers Spark SQL (donne accès aux dataframes, aux RDD,
    // création de tables temporaires, etc., et donc aux mécanismes de distribution des calculs)
    val spark = SparkSession
      .builder
      .config(conf)
      .appName("Projet Gdelt : Query 3")
      .getOrCreate()

    val sc = new SparkContext(conf)

    import spark.implicits._

    val AWS_ID = "TODO"
    val AWS_KEY = "TODO"
    val AWS_TOKEN = "TODO"
    val s3_name = "projet-gdelt-2019"

    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    sc.hadoopConfiguration.set("fs.s3a.access.key", AWS_ID) // mettre votre ID du fichier credentials.csv
    sc.hadoopConfiguration.set("fs.s3a.secret.key", AWS_KEY) // mettre votre secret du fichier credentials.csv
    sc.hadoopConfiguration.set("fs.s3a.session.token", AWS_TOKEN)
    sc.hadoopConfiguration.setInt("fs.s3.maxConnections", 1000)

    /** **************************************
     * Charger un fichier csv dans un rdd depuis s3
     * ********************************************/

    val RDDgkg: RDD[String] = sc.binaryFiles("s3a://" + s3_name + "/2018120105*.gkg.csv.zip").
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

    val dfgkg: DataFrame = RDDgkg.toDF.withColumn("GLOBALEVENTID", split($"value", "\\t").getItem(0))
      .withColumn("GKGRECORDID", split($"value", "\\t").getItem(0))
      .withColumn("DATE", split($"value", "\\t").getItem(1))
      .withColumn("SourceCollectionIdentifier", split($"value", "\\t").getItem(2))
      .withColumn("source_common_name", split($"value", "\\t").getItem(3))
      .withColumn("DocumentIdentifier", split($"value", "\\t").getItem(4))
      .withColumn("Counts", split($"value", "\\t").getItem(5))
      .withColumn("V2Counts", split($"value", "\\t").getItem(6))
      .withColumn("Themes", split($"value", "\\t").getItem(7))
      .withColumn("V2Themes", split($"value", "\\t").getItem(8))
      .withColumn("Locations", split($"value", "\\t").getItem(9))
      .withColumn("V2Locations", split($"value", "\\t").getItem(10))
      .withColumn("Persons", split($"value", "\\t").getItem(11))
      .withColumn("V2Persons", split($"value", "\\t").getItem(12))
      .withColumn("Organizations", split($"value", "\\t").getItem(13))
      .withColumn("V2Organizations", split($"value", "\\t").getItem(14))
      .withColumn("V2Tone", split($"value", "\\t").getItem(15))
      .withColumn("Dates", split($"value", "\\t").getItem(16))
      .withColumn("GCAM", split($"value", "\\t").getItem(17))
      .withColumn("SharingImage", split($"value", "\\t").getItem(18))
      .withColumn("RelatedImages", split($"value", "\\t").getItem(19))
      .withColumn("SocialImageEmbeds", split($"value", "\\t").getItem(20))
      .withColumn("SocialVideoEmbeds", split($"value", "\\t").getItem(21))
      .withColumn("Quotations", split($"value", "\\t").getItem(22))
      .withColumn("AllNames", split($"value", "\\t").getItem(23))
      .withColumn("Amounts", split($"value", "\\t").getItem(24))
      .withColumn("TranslationInfo", split($"value", "\\t").getItem(25))
      .withColumn("Extras", split($"value", "\\t").getItem(26))
      .drop("value")


    /** ******************************************************
     * Creation of dataframes that will be inserted in Cassandra
     * **********************************************************/

    val df_article_by_theme: DataFrame = dfgkg
      .select("GKGRECORDID", "source_common_name", "Themes", "V2Tone", "DATE")
      .withColumn("theme", explode(split($"Themes", ";")))
      .filter(!($"theme".isNaN || $"theme".isNull || $"theme" === ""))
      .withColumn("Tone", substring_index($"V2Tone", ",", 1))
      .withColumn("year", substring($"DATE", 0, 4))
      .withColumn("month", substring($"DATE", 5, 2))
      .withColumn("day", substring($"DATE", 7, 2))
      .groupBy("source_common_name","theme", "year", "month", "day")
      .agg(count($"GKGRECORDID").alias("num_article"),
        sum($"tone").alias("sum_tone"))

    val df_article_by_person: DataFrame = dfgkg
      .select("GKGRECORDID", "source_common_name","Persons", "V2Tone", "DATE")
      .withColumn("person", explode(split($"Persons", ";")))
      .filter(!($"person".isNaN || $"person".isNull || $"person" === ""))
      .withColumn("Tone", substring_index($"V2Tone", ",", 1))
      .withColumn("year", substring($"DATE", 0, 4))
      .withColumn("month", substring($"DATE", 5, 2))
      .withColumn("day", substring($"DATE", 7, 2))
      .groupBy("source_common_name","person", "year", "month", "day")
      .agg(count($"GKGRECORDID").alias("num_article"),
        sum($"tone").alias("sum_tone"))

    val df_article_by_location: DataFrame = dfgkg
      .select("GKGRECORDID", "source_common_name", "V2Locations", "V2Tone", "DATE")
      .withColumn("Locations", explode(split($"V2Locations", ";")))
      .filter(!($"Locations".isNaN || $"Locations".isNull || $"Locations" === ""))
      .withColumn("location", element_at(split($"Locations", "#"),2))
      .withColumn("Tone", substring_index($"V2Tone", ",", 1))
      .withColumn("year", substring($"DATE", 0, 4))
      .withColumn("month", substring($"DATE", 5, 2))
      .withColumn("day", substring($"DATE", 7, 2))
      .groupBy("source_common_name","location", "year", "month", "day")
      .agg(count($"GKGRECORDID").alias("num_article"),
        sum($"tone").alias("sum_tone"))


    /***********************************
     * Save Calculation in S3
     ******************************/

    df_article_by_theme
      .write
      .mode(SaveMode.Overwrite)
      .parquet("s3://" + s3_name + "/article_by_theme.parquet/")

    df_article_by_person
      .write
      .mode(SaveMode.Overwrite)
      .parquet("s3://" + s3_name + "/article_by_person.parquet/")

    df_article_by_location
      .write
      .mode(SaveMode.Overwrite)
      .parquet("s3://" + s3_name + "/article_by_location.parquet/")


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
           WITH REPLICATION ={
             'class': 'NetworkTopologyStrategy',
             'us-east': 2 };
        """)
      session.execute(
        """
           CREATE TABLE IF NOT EXISTS gdelt.article_by_theme (
              source_common_name text,
              year int,
              month int,
              day int,
              theme text,
              num_article int,
              sum_tone int,
              PRIMARY KEY (source_common_name, year, month, day, theme)
            );
        """
      )
      session.execute(
        """
          CREATE TABLE IF NOT EXISTS gdelt.article_by_person (
              source_common_name text,
              year int,
              month int,
              day int,
              person text,
              num_article int,
              sum_tone int,
              PRIMARY KEY (source_common_name, year, month, day, person)
          );
        """
      )
      session.execute(
        """
          CREATE TABLE IF NOT EXISTS gdelt.article_by_location (
              source_common_name text,
              year int,
              month int,
              day int,
              person text,
              num_article int,
              sum_tone int,
              PRIMARY KEY (source_common_name, year, month, day, person)
          );
        """
      )
    }

    /*************************
     * Import of the data
     *************************/
    df_article_by_theme.write
      .cassandraFormat("article_by_theme", "gdelt")
      .option(CassandraConnectorConf.)
      .save()

    df_article_by_person.write
      .cassandraFormat("article_by_person", "gdelt")
      .save()

    df_article_by_location.write
      .cassandraFormat("article_by_location", "gdelt")
      .save()

    /*****************
     * Read Data
     ***************/

    val article_by_theme_test = spark.read
      .cassandraFormat("article_by_theme", "gdelt")
      .load()

  }
}