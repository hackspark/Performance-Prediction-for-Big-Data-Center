import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.math._



/**
 * Created by midsummer on 2015/8/15.
 */

  object Gmf {
    def main(args:Array[String]) {
//      val conf = new SparkConf().setAppName("Gmf").setMaster("spark://11.11.49.90:7077").set("spark.driver.host", "midsummer-pc")
//        .setJars(List("D:\\IdeaProjects\\Gmf\\out\\artifacts\\Gmf_jar\\Gmf.jar")).set("spark.executor.memory", "30G")
//        .set("spark.cores.max", "50")
      val conf =new SparkConf()
      val sc = new SparkContext()
      //basetable（jobid,machine,performance,starttime,endtime,cpi）
      val useage = sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/task_usage")
        .map(x => (x.split(",")(2), x.split(",")(4), x.split(",")(0), x.split(",")(1), x.split(",")(15))).cache()
     //jobid group
      val jobid = sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/job_events").map(x=> x.split(",")(2)).distinct. zipWithIndex.toLocalIterator.toMap

     //machineid group
      val machine = sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/machine_events").filter(_.split(",").size==6)
       .map(x=> (x.split(",")(4),x.split(",")(5))).distinct.zipWithIndex.toLocalIterator.toMap

      val machineid = sc.textFile("hdfs://11.11.49.90:9000/data/googlefile/machine_events").filter(_.split(",").size==6)
        .map(x=> (x.split(",")(1),machine(x.split(",")(4),x.split(",")(5)))).toLocalIterator.toMap
//
//      val machine_id = (machineid join machine).map(x=> x._2).distinct.cache()


     //basetper normalized (jobid ,machine ,x-min/max-min) time
      val usetper = useage.map(x=> ((x._1,x._2),x._4.toDouble-x._3.toDouble)).groupByKey.map(x=>(x._1,(0.0 /:x._2)(_+_),x._2.size)).map(x=> (x._1,x._2/x._3)).cache()
      val mint = usetper.map(x=> x._2).min
      val maxt = usetper.map(x=> x._2).max
      val comptper = usetper.map(x=> (x._1, (x._2-mint)/(maxt-mint))).cache()
      //format basetper

      val basetper =comptper.map(x=> (jobid(x._1._1),machineid(x._1._2),x._2))
        .map(x=> ((x._1,x._2),x._3)).groupByKey.map(x=>(x._1,(0.0 /:x._2)(_+_),x._2.size)).map(x=> (x._1._1,x._1._2,x._2/x._3))

      //basetper normalized (jobid.machine,x-ave/stdev) time
      val avetper = usetper.map(_._2).sum/usetper.map(_._2).count
      val stdevtper = sqrt(usetper.map(x=> pow(x._2-avetper,2)).sum/usetper.map(x=> x._2).count)
      val compstper = usetper.map(x=> (x._1, (x._2-avetper)/stdevtper)).cache()
      //format basestper
      val basestper = compstper.map(x=> (jobid(x._1._1),machineid(x._1._2),x._2))
        .map(x=> ((x._1,x._2),x._3)).groupByKey.map(x=>(x._1,(0.0 /:x._2)(_+_),x._2.size)).map(x=> (x._1._1,x._1._2,x._2/x._3))
      //basecpi normalized (jobid , machine , x-min/max-min ) cpi
      val usecpi = useage.filter(_._5 != "").map(x=> ((x._1,x._2),x._5.toDouble)).groupByKey.map(x=>(x._1,(0.0 /:x._2)(_+_),x._2.size)).map(x=> (x._1,x._2/x._3)).cache()
      val minc = usecpi.map(x=> x._2).min
      val maxc = usecpi.map(x=> x._2).max
      val compcper = usecpi.map(x=> (x._1, (x._2-minc)/(maxc-minc))).cache()
      //format basecper
      val basecper = compcper.map(x=> (jobid(x._1._1),machineid(x._1._2),x._2))
        .map(x=> ((x._1,x._2),x._3)).groupByKey.map(x=>(x._1,(0.0 /:x._2)(_+_),x._2.size)).map(x=> (x._1._1,x._1._2,x._2/x._3))
      //basetper normalized (jobid.machine,x-ave/stdev) cpi
      val avecper = usecpi.map(_._2).sum/usecpi.map(_._2).count
      val stdevcper = sqrt(usecpi.map(x=> pow(x._2-avecper,2)).sum/usecpi.map(x=> x._2).count)
      val compscper = usecpi.map(x=> (x._1,(x._2-avecper)/stdevcper )).cache()
      //format basecper
      val basescper = compscper.map(x=> (jobid(x._1._1),machineid(x._1._2),x._2))
        .map(x=> ((x._1,x._2),x._3)).groupByKey.map(x=>(x._1,(0.0 /:x._2)(_+_),x._2.size)).map(x=> (x._1._1,x._1._2,x._2/x._3))

      basetper.map(x =>  x._1 + "," +x._2+","+ x._3 ).repartition(5).saveAsTextFile("hdfs://11.11.49.90:9000/data/googlefile/basetper")
      basestper.map(x =>  x._1+ "," +x._2+ "," + x._3 ).repartition(5).saveAsTextFile("hdfs://11.11.49.90:9000/data/googlefile/basestper")
      basecper.map(x =>  x._1 + ","+ x._2 +","+ x._3 ).repartition(5).saveAsTextFile("hdfs://11.11.49.90:9000/data/googlefile/basecper")
      basescper.map(x => x._1 +","+x._2 + "," + x._3 ).repartition(5).saveAsTextFile("hdfs://11.11.49.90:9000/data/googlefile/basescper")






//      basetper.take(10) foreach println
//      println(basetper.count)
    }
}
