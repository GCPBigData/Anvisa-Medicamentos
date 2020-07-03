package SparkTest

import org.apache.spark.sql.SparkSession

/**
 * CSV ingestion in a dataframe.
 * Parquet Save.
 *
 * @author web2ajax@gmail.com
 */
object SparkBuildTest {
  val spark = SparkSession.builder()
    .appName("Test App")
    .master("local[2]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    spark.sql("select 'hello' as h, 'hi' as b").show()
  }
}
