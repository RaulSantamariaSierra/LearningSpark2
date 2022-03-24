import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, desc, length, lit, sum, trim}
import org.apache.spark
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

object Padron extends App{

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()


  var leer = sparkSession.read
    .option("header", "true")
    .option("sep", ";")
    .option("inferSchema", "true")
    .csv("src/main/resources/Padron/Rango_Edades_Seccion_202203.csv")

  val bar = leer.select(col("DESC_BARRIO")).distinct()

  leer.createTempView("padron")
  sparkSession.sql("SELECT COUNT(DISTINCT DESC_BARRIO) FROM padron")

  /*
    val long = leer.withColumn("Longitud",functions.length(col("DESC_DISTRITO")))


    val col = long.withColumn(col("Val5"), lit(5))

    val del = col.drop("Val5")

    del.write.mode("overwrite")
      .partitionBy("DESC_DISTRITO","DESC_BARRIO")
      .csv("src/main/resources/Padron/Particionado")

  */

  val num =leer.select("DESC_DISTRITO","DESC_BARRIO","EspanolesHombres","EspanolesMujeres","ExtranjerosHombres","ExtranjerosMujeres")
    .groupBy("DESC_DISTRITO","DESC_BARRIO")
    .agg(sum("EspanolesHombres").as("EspanolesHombres")
      ,sum("EspanolesMujeres").as("EspanolesMujeres")
      ,sum("ExtranjerosHombres").as("ExtranjerosHombres")
      ,sum("ExtranjerosMujeres").as("ExtranjerosMujeres"))
    .orderBy(desc("ExtranjerosMujeres"),desc("ExtranjerosHombres")).show(5)

  val uni = leer
    .select(col("DESC_DISTRITO"), col("DESC_BARRIO"),col("EspanolesHombres"))
    .groupBy(col("DESC_DISTRITO"),col("DESC_BARRIO"))
    .agg(sum("EspanolesHombres").as("SumEspHombres"))

  uni.show()


  val join = leer.join(uni,leer("DESC_DISTRITO") === uni("DESC_DISTRITO") && leer("DESC_BARRIO") === uni("DESC_BARRIO"))
    .orderBy(col("COD_DISTRITO"),col("COD_DIST_BARRIO"))

  join.show()


  val df = leer.select(col("DESC_DISTRITO"),col("COD_EDAD_INT")).where(col("COD_EDAD_INT").isNotNull).distinct().show()

  val pivotPadron = join.where(col("DESC_DISTRITO") === "BARAJAS" || col("DESC_DISTRITO") === "CENTRO" || col("DESC_DISTRITO") === "RETIRO")
    .groupBy("COD_EDAD_INT").pivot("DESC_DISTRITO").sum("EspanolesMujeres").orderBy("COD_EDAD_INT")

  pivotPadron.show()

  val pivot = leer
    .where(col("DESC_DISTRITO").equalTo("CENTRO") || col("DESC_DISTRITO").equalTo("BARAJAS") || col("DESC_DISTRITO").equalTo("RETIRO"))
    .groupBy(col("COD_EDAD_INT"))
    .pivot("DESC_DISTRITO").sum("EspanolesMujeres")
    .orderBy("COD_EDAD_INT")

  pivot.show()



  leer.write.mode("overwrite")
    .partitionBy("DESC_DISTRITO","DESC_BARRIO")
    .csv("src/main/resources/salida/padron_csv")

  leer.write.mode("overwrite")
    .partitionBy("DESC_DISTRITO","DESC_BARRIO")
    .parquet("src/main/resources/salida/padron_parquet")



}