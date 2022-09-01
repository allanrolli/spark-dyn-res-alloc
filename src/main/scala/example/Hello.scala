package example

// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

// define main method (Spark entry point)
object Hello {

  def main(args: Array[String]) = {
    // Activate speculative task
    val conf = new SparkConf().setAppName("Spark dynamic allocation demo")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.dynamicAllocation.initialExecutors", "2")
      .set("spark.dynamicAllocation.executorIdleTimeout", "60s")
      .set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
      .set("spark.executor.cores", "2")
      .set("spark.executor.memory", "15g")

    val sparkContext = new SparkContext(conf)

    println("Starting processing")
    sparkContext.parallelize(0 to 30, 30)
      .foreach(item => {
        // for each number wait 3 seconds
        Thread.sleep(30000)
      })
    println("Terminating processing")
  }
}