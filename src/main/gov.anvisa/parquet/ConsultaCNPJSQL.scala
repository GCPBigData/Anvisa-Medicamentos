package parquet

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 *
 * Consulta CNPJ Tabela Spark
 *
 * fonte de dados : https://dados.anvisa.gov.br/dados/
 *
 * @author web2ajax@gmail.com - 03/07/2020
 *
 * https://github.com/GCPBigData/Anvisa-Medicamentos
 */
object ConsultaCNPJSQL extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("Consulta CNPJ")
      .master("local[*]")
      .getOrCreate

    // Ler o parquet e cria uma view temporaria.
    val TA_PAF_DF = ss.read
      .format("paquet")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src\\main\\resources\\data\\TA_PAF\\TA_PAF.parquet")
      .createOrReplaceTempView("View_TA_PAF_DF")

    // Ler a View com SQL
    val TA_PAF_DF_SQL = ss.sql("SELECT * FROM View_TA_PAF_DF LIMIT 1")
    TA_PAF_DF_SQL.show()
    
    logger.info("===========Finished=========")
    ss.stop()
  }
}