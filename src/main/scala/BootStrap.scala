import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ListBuffer

object BootStrap {

  def main(args: Array[String]): Unit = {

  val conf = new SparkConf().setMaster("local[*]").setAppName("BootStrap")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd = sc.textFile("Wages1.csv")

    val population = rdd.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }.map(x => (x.split(",")(1),x.split(",")(4))).cache()

    val sqlContext= new SQLContext(sc)
    import sqlContext.implicits._

    val meanVar = population.toDF("Experience", "Wage")

    println("Showing original Data Statistics")
    meanVar.groupBy(col("Experience")).agg(stddev("Wage"),mean("Wage")).show()

    val list = new ListBuffer[Array[(String, String, String)]]()

    val takeSample =  population.sample(false,0.25).cache()

     for(a <- 1 to 25){
       val resampledData = takeSample.sample(true,1.0).cache()

       val meanVarResample = resampledData.toDF("Experience", "Wage")

      val backToRdd = meanVarResample.groupBy(col("Experience")).agg(stddev("Wage"),mean("Wage")).map(row => (Tuple3(row(0).toString,row(1).toString,row(2).toString))).filter(x => !x._2.toDouble.isNaN && !x._3.toDouble.isNaN)

       list += backToRdd.collect()

     }

    val listToRDD = sc.parallelize(list).flatMap(x => x)

    val resultRDD = listToRDD.map(x => (x._1,(x._2.toDouble,x._3.toDouble))).reduceByKey((x,y) => (x._1.toDouble + y._1.toDouble,x._2.toDouble + y._2.toDouble)).mapValues((values) => (values._1.toDouble/25,values._2.toDouble/25)).map((key => Tuple3(key._1,key._2._1.toString,key._2._2.toString)))

    println("Showing reSampled Data Statistics")
    resultRDD.toDF("Experience", "Variance Wage", "Mean Wage").show()



  }
}
