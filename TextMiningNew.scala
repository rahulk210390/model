package sql

import java.io.File
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StringType, StructField, DoubleType, LongType, IntegerType, DateType, DataType }
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._
import java.text.SimpleDateFormat
import java.text.ParseException
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import System.getProperties
import java.util.Date
import java.util.Calendar
import org.apache.spark.mllib.clustering.{ LDA, DistributedLDAModel }
import org.apache.spark.mllib.linalg.Vectors
import java.io.IOException
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import scala.collection.mutable
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel, LocalLDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable

object TextMiningNew {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").setAppName("SparkDFebay")

    System.setProperty("hadoop.home.dir", "D:/hadooponwindows-master")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import scala.io._

    

    val rdd = sc.textFile("C:/Data/RepNodesdata.txt")

    val schemaString = "id seperator text"
    
    
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    
    
    val rowRDD = rdd.map(_.split("\t")).map(e ⇒ Row(e(0), e(1), e(2)))

    val newRow = sqlContext.createDataFrame(rowRDD, schema).toDF("Id", "Seperator", "Text")

    
    import org.apache.spark.ml.feature.StopWordsRemover
    import org.apache.spark.ml.feature.Tokenizer
    import org.apache.spark.ml.feature.StopWordsRemover
    import org.apache.spark.sql.DataFrame

    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("Text")
      .setOutputCol("tokens_raw")

    val remover: StopWordsRemover = new StopWordsRemover()
      .setInputCol("tokens_raw")
      .setOutputCol("stopwordremoved")

    val tokenized: DataFrame = tokenizer.transform(newRow)
    
    
    val filtered: DataFrame = remover.transform(tokenized)
    
    
    filtered.show()

    

         
    val content = filtered.select("stopwordremoved").flatMap ( x => x.getAs[Seq[String]](0))
    content.collect().foreach(println)
    
    val show =  content.toArray().toList
    
    
   // println(show)
    
    
    def processLine(s: String): List[String] =
      {

        s.toUpperCase().replaceAll("[0-9]", "").replaceAll("[\\s.:;&=<>/$*+%`@#()!||'?-]", " ").replace("\"", " ").replaceAll("( )+", " ").trim()
          .split("\\s+")
          .toList
      }
  
    val filtered_doc_words = show.map(processLine(_))

    val dict_list = Source.fromFile("C:/Data/dict.txt").getLines.toList

    val tokens = dict_list.map(x => x.split("\t"))
    
    val dict: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()

    for (keyval <- tokens) { dict += (keyval(0) -> keyval(1)) }
    val final_data = filtered_doc_words.map(line => { var ln = line.toString(); for (x <- dict.keys) { ln = ln.replaceAll(x, dict(x)) }; ln })

    val line2 = final_data.flatMap(x => x.split("\n"))
    
    print(line2)
    
    val finalrdd = sc.parallelize(Seq(line2))
    
    val RowRDD = finalrdd.map(r => Row.fromSeq(r).toString())
    
   RowRDD.collect().foreach(println)
          

     println("-----------------------")
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(finalrdd)
    
    tf.collect().foreach (println)
    
     println("-----------------------")
     
     
      import org.apache.spark.mllib.feature.IDF
 println("-----------------------")
tf.cache()
val idf = new IDF().fit(tf)
val tfidf: RDD[Vector] = idf.transform(tf)
tfidf.collect().foreach (println)
val tdidf_withindex = tfidf.zipWithIndex()

tdidf_withindex.collect().foreach (println)


val corpus = tfidf.zipWithIndex.map(_.swap).cache()




val tokenized1: RDD[Seq[String]] =   content.map(_.toUpperCase().replaceAll("[0-9]", "").replaceAll("[\\s.:;&=<>/$*+%`@#()!||'?-]", " ").replace("\"", " ").replaceAll("( )+", " ").trim().split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))


val termCounts: Array[(String, Long)] =   tokenized1.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

val numStopwords = 20

val vocabArray: Array[String] =   termCounts.takeRight(termCounts.size - numStopwords).map(_._1)

val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
val documents: RDD[(Long, Vector)] =
  tokenized1.zipWithIndex.map { case (tokens, id) =>
    val counts = new mutable.HashMap[Int, Double]()
    tokens.foreach { term =>
      if (vocab.contains(term)) {
        val idx = vocab(term)
        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
      }
    }
(id, Vectors.sparse(vocab.size, counts.toSeq))
 }
    
val ldaModel: DistributedLDAModel = new LDA().setK(10).setMaxIterations(20).run(documents).asInstanceOf[DistributedLDAModel]
val localLDAModel: LocalLDAModel = ldaModel.toLocal
    val topicDistributions = localLDAModel.topicDistributions(documents)
    
    println("first topic distribution:"+topicDistributions.first._2.toArray.mkString(", "))

  val topicIndices = localLDAModel.describeTopics(maxTermsPerTopic = 20)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
   
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term              $weight")
      }
      println()
    }
}
}  