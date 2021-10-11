import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_source_0_out0        = source_0(spark)
    val df_reformat_0_out_0     = reformat_0(spark,     df_source_0_out0)
    val df_my_new_limit_0_out_0 = my_new_limit_0(spark, df_reformat_0_out_0)
    val df_filter_0_out_0       = filter_0(spark,       df_my_new_limit_0_out_0)
    val df_filter_1_out_0       = filter_1(spark,       df_filter_0_out_0)
    df_filter_1_out_0.cache().count()
    df_filter_1_out_0.unpersist()
  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Workflow")
      .config("spark.default.parallelism", "4")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    apply(spark)
  }

}
