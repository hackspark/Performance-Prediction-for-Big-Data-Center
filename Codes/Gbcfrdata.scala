/**
 * Created by midsummer on 2015/8/15.
 */


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.math._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
object Gbcfrdata {
  def main(args: Array[String]) {

    val vrank = args(1).toInt
    val vnum = args(2).toInt
    val vlambda = args(3).toDouble
    val dir = args(4)

//    val conf = new SparkConf().setAppName("Gmf").setMaster("spark://11.11.49.90:7077").set("spark.driver.host", "midsummer-pc")
//      .setJars(List("D:\\IdeaProjects\\Gbcfrdata\\out\\artifacts\\Gbcfrdata_jar\\Gbcfrdata.jar")).set("spark.executor.memory", "5G")
//      .set("spark.cores.max", "50")
    val conf =new SparkConf()
    val sc = new SparkContext()
    val file = {
      args(0).toInt match {
        case 1 => sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/basetper")
        case 2 => sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/basestper")
        case 3 => sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/basecper")
        case 4 => sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/basescper")
      }
    }
    val ratings = file.map(x => Rating(x.split(",")(0).toInt, x.split(",")(1).toInt, x.split(",")(2).toDouble))

    val rank = vrank
    val numIterations = vnum
    val lambda = vlambda
    val model = ALS.train(ratings, rank, numIterations, lambda)
    // Evaluate the model on rating data
    val pjob = ratings.map { case Rating(jobid, machine, performance) =>
      (jobid, machine)
    }
    val predictions =
      model.predict(pjob).map { case Rating(jobid, machine, performance) =>
        ((jobid, machine), performance)
      }
    val ratesAndPreds = ratings.map { case Rating(jobid, machine, performance) =>
      ((jobid, machine), performance)
    }.join(predictions)

    val finaldata = ratesAndPreds.map { case ((jobid, machine), (r1, r2)) =>
      (abs(r1 - r2) / r1,(jobid, machine,r1,r2))
    }.sortByKey(false)
    val topdata = finaldata.zipWithIndex().filter(x => x._2 < 10000)
      .map(x=>  x._1._2._1+","+x._1._2._2+","+x._1._2._3+","+x._1._2._4)
      .saveAsTextFile("hdfs://11.11.49.90:9000/data/googlefile/" + dir)
  }


}
