import java.lang

import org.apache.spark
import org.apache.spark.sql.delta.DeltaTable
import org.apache.spark.sql.{ Dataset, SaveMode, SparkSession }

object example extends App{
  val Spark = SparkSession.builder()
    .appName("Spark Delta example")
    .master("local")
    .config("spark.executor.instances", "2")
    .getOrCreate()

  import Spark.implicits._

  Spark.range(0,5)
    .write
    .format("delta")
    .mode(SaveMode.Overwrite)
    .save(new java.io.File("./tmp/delta-table/").getCanonicalPath)

  def readTable(path:String)= {
    val df = Spark.read
      .format("delta")
      .load(path)
    df.show()
  }

  def updateTableData(data:Dataset[Long],path:String)={
    data.write
      .format("delta")
      .mode("overwrite")
      .save(path)
  }


  //Time Travel

  val path = new java.io.File("./tmp/delta-table/").getCanonicalPath
  val data = Spark.range(0, 5)
  data.write.format("delta").mode("overwrite").save(path)

  val moreData = Spark.range(20, 25)
  moreData.write.format("delta").mode("overwrite").save(path)

  val moreData1 = Spark.range(22, 27)
  moreData1.write.format("delta").mode("overwrite").save(path)


  Spark.read.format("delta").option("versionAsOf", 0).load(path).show()

  Spark.read.format("delta").option("versionAsOf", 1).load(path).show()

}
