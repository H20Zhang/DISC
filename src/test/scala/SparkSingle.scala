import org.apache.spark.sql.SparkSession

object SparkSingle {
  private lazy val spark = SparkSession.builder().master("local[1]").appName("spark sql example").config("spark.some.config.option", "some-value")
    .getOrCreate()
  private lazy val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  var counter = 0

  def getSpark() = {
    counter += 1
    (spark,sc)
  }




  def close(): Unit ={
    counter -= 1
    if(counter == 0){
      spark.close()
    }
  }

}
