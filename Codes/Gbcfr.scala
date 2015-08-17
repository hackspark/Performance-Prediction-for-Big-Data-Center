import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.math._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

/**
 * Created by midsummer on 2015/8/15.
 */


object Gbcfr {
  def main(args:Array[String]) {

    val vrank = args(1).toInt
    val vnum = args(2).toInt
    val vlambda = args(3).toDouble

//    val conf = new SparkConf().setAppName("Gmf").setMaster("spark://11.11.49.90:7077").set("spark.driver.host", "midsummer-pc")
//      .setJars(List("D:\\IdeaProjects\\Gbcfr\\out\\artifacts\\Gbcfr_jar\\Gbcfr.jar")).set("spark.executor.memory", "5G")
//      .set("spark.cores.max", "50")
        val conf =new SparkConf()
    val sc = new SparkContext()
    val file = {args(0).toInt match{
      case 1 => sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/basetper")
      case 2 => sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/basestper")
      case 3 => sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/basecper")
      case 4 => sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/basescper")
    }
    }
    val ratings= file.map(x => Rating(x.split(",")(0).toInt, x.split(",")(1).toInt, x.split(",")(2).toDouble))
    val splits = ratings.randomSplit(Array(0.9, 0.1), seed = 111)
    val training = splits(0)
    val test = splits(1)


//     Build the recommendation model using ALS
    val rank = vrank
    val numIterations = vnum
    val lambda = vlambda
    val model = ALS.train(training,rank, numIterations,lambda)
    // Evaluate the model on rating data

    val pjob = test.map { case Rating(jobid, machine, performance) =>
      (jobid, machine)
    }
    val predictions =
      model.predict(pjob).map { case Rating(jobid, machine, performance) =>
        ((jobid, machine), performance)
      }
    val ratesAndPreds = test.map { case Rating(jobid, machine, performance) =>
      ((jobid, machine), performance)
    }.join(predictions)

    val MSE = ratesAndPreds.map { case ((jobid, machine), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
//    println("Mean Squared Error = " + MSE)
    val Rmse = sqrt(MSE)
    println( "Rank = "+rank+","+"Iteration ="+numIterations+","+"Lambda ="+vlambda+","+ "RMSE by Time =" + Rmse)

//    ratesAndPreds.repartition(1).saveAsTextFile("hdfs://11.11.49.90:9000/data/googlefile/"+dir)
  }
}





