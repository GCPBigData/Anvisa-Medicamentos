package parquet

import org.apache.spark.sql.SparkSession
import spark.sql

/**
 *
 * Converte Parquet para Tabela Spark
 *
 * fonte de dados : https://dados.anvisa.gov.br/dados/
 * Converte Todos os parquet para Tabela Sqpark.
 * Dataset separados por ano.
 *
 * @author web2ajax@gmail.com - 03/07/2020
 *
 * https://github.com/GCPBigData/Anvisa-Medicamentos
 */
object CriaTabelaSpark extends Serializable {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .appName("CSV to Dataset")
      .master("local[*]")
      .getOrCreate

    // Ler os arquivos parquetconvertidos
    val TA_PAF_ParquetDF = ss.read
        .format("parquet")
        .option("path", "src\\main\\resources\\data\\TA_PAF\\TA_PAF.parquet")
        .load()

    // DDL para criação do banco de dados ANVISA
    sql("CREATE DATABASE IF NOT EXISTS ANVISA")
    sql("USE ANVISA")

    // Cria a tabela Spark em Formato CSV
    TA_PAF_ParquetDF.write
        .mode(SaveMode.Overwrite)
        .format("csv")
        .bucketBy("NU_CNPJ_EMPRESA")
        .sortBy("ANVISA.DT_ENTRADA")

    // Lista as Tabelas
    ss.catalog.listTables("ANVISA").show()

    //por logger
    ss.stop()
  }
}
