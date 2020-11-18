package me.assignment

import me.assignment.conf.{AssignmentParameters, LocalInput, LocalOutput, TestInput, Weekly}
import me.assignment.AssignmentJobTests._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec

import collection.JavaConverters._

class AssignmentJobTests extends AnyFlatSpec with PrivateMethodTester {

  private def doDivisionFilteringTest(divisions: String*): DataFrame = {
    val job = new AssignmentJob(AssignmentParameters(dummyInput, Seq(divisions: _*), Weekly, dummyOutput))
    val fn = PrivateMethod[DataFrame]('filterSelectedDivisions)
    val sales = spark.createDataFrame(Seq(
      Row("A", "product1"),
      Row("B", "product1"),
      Row("C", "product2"),
      Row("D", "product3")
    ).asJava, divisionFilterSalesSchema)
    val product = spark.createDataFrame(Seq(
      Row("product1", "division1"),
      Row("product2", "division2"),
      Row("product3", "division3")
    ).asJava, divisionFilterProductSchema)
    job invokePrivate fn(sales, product)
  }

  "Division Filtering" should "be a no-op when no divisions are selected" in {
    val df = doDivisionFilteringTest()
    assert(df.count() == 4)
  }

  it should "remove sales for non-selected divisions" in {
    val df = doDivisionFilteringTest("division2", "division3")
    assert(df.count() == 2)

    val errorEntries = df.filter(col("productId").equalTo(lit("product1")))
    assert(errorEntries.count() == 0, "There were sales for product1 left in the dataframe")
  }

  it should "return an empty dataframe when the selected divisions do not exist" in {
    val df = doDivisionFilteringTest("non-existent-division")
    assert(df.count() == 0)
  }

  "Initial Aggregation" should "correctly sum the values per week" in {
    val job = new AssignmentJob(AssignmentParameters(dummyInput, Seq(), Weekly, dummyOutput))
    val fn = PrivateMethod[DataFrame]('aggregateStores)
    val sales = spark.createDataFrame(Seq(
      Row(1, 1, "product1", 4.5, 3),
      Row(1, 1, "product1", 6.0, 4),
      Row(1, 2, "product1", 5.0, 5)
    ).asJava, aggregationSalesSchema)

    val calendar = spark.createDataFrame(Seq(
      Row(1, 2020, "W11"),
      Row(2, 2020, "W12")
    ).asJava, aggregationCalendarSchema)

    val df = job invokePrivate fn(sales, calendar)
    val values = df.collect()

    val w11Row = values.find(_.getAs[String]("weekTag") == "W11") match {
      case Some(value) => value
      case None => fail("No result for W11")
    }

    val w12Row = values.find(_.getAs[String]("weekTag") == "W12") match {
      case Some(value) => value
      case None => fail("No result for W11")
    }

    assert(w11Row.getAs[Double]("netSales") == 10.5)
    assert(w11Row.getAs[Long]("salesUnits") == 7)

    assert(w12Row.getAs[Double]("netSales") == 5.0)
    assert(w12Row.getAs[Long]("salesUnits") == 5)
  }

  it should "not crash on empty data" in {
    val job = new AssignmentJob(AssignmentParameters(dummyInput, Seq(), Weekly, dummyOutput))
    val fn = PrivateMethod[DataFrame]('aggregateStores)
    val sales = spark.createDataFrame(Seq[Row]().asJava, aggregationSalesSchema)
    val calendar = spark.createDataFrame(Seq[Row]().asJava, aggregationCalendarSchema)

    val df = job invokePrivate fn(sales, calendar)

    assert(df.count() == 0)
  }

  "makeUniqueKey" should "create correct upper-cased keys" in {
    val job = new AssignmentJob(AssignmentParameters(dummyInput, Seq(), Weekly, dummyOutput))
    val fn = PrivateMethod[Column]('makeUniqueKey)
    val inDF = spark.createDataFrame(Seq(
      Row(2020, "channel", "division", "gender", "category")
    ).asJava, uniqueKeySchema)
    val outDF = inDF.withColumn("uniqueKey", job invokePrivate fn(inDF))
    val row = outDF.collect().headOption match {
      case Some(value) => value
      case None => fail("No result from makeUniqueKey")
    }
    assert(row.getAs[String]("uniqueKey") == "2020_CHANNEL_DIVISION_GENDER_CATEGORY")
  }

  "addMissingWeeks" should "fill 0's for weeks without data" in {
    val job = new AssignmentJob(AssignmentParameters(dummyInput, Seq(), Weekly, dummyOutput))
    val fn = PrivateMethod[DataFrame]('addMissingWeeks)
    val agg = spark.createDataFrame(Seq(
      Row(1, "product", 2020, "W1", 10.0, 2),
      Row(2, "product", 2020, "W1", 20.0, 4),
      Row(2, "product", 2020, "W2", 10.0, 2),
    ).asJava, missingWeeksAggSchema)
    val store = spark.createDataFrame(Seq(
      Row(1, "Digital"),
      Row(2, "Store")
    ).asJava, missingWeeksStoreSchema)
    val product = spark.createDataFrame(Seq(
      Row("product", "APPAREL", "MENS", "BASEBALL")
    ).asJava, missingWeeksProductSchema)

    val df = job invokePrivate fn(agg, store, product)
    val keyARows = df.filter(col("uniqueKey").equalTo(lit("2020_DIGITAL_APPAREL_MENS_BASEBALL")))
    val keyBRows = df.filter(col("uniqueKey").equalTo(lit("2020_STORE_APPAREL_MENS_BASEBALL")))

    assert(keyARows.count() == 52)
    assert(keyBRows.count() == 52)
  }
}

object AssignmentJobTests {
  private val dummyInput = TestInput()
  private val dummyOutput = LocalOutput("dummyOutput", "json")
  private val spark = SparkSession.builder().master("local[2]").getOrCreate()

  private val divisionFilterSalesSchema = StructType(Seq(
    StructField("id", StringType),
    StructField("productId", StringType)
  ))

  private val divisionFilterProductSchema = StructType(Seq(
    StructField("productId", StringType),
    StructField("division", StringType)
  ))

  private val aggregationSalesSchema = StructType(Seq(
    StructField("storeId", IntegerType),
    StructField("dateId", IntegerType),
    StructField("productId", StringType),
    StructField("netSales", DoubleType),
    StructField("salesUnits", IntegerType)
  ))

  private val aggregationCalendarSchema = StructType(Seq(
    StructField("dateId", IntegerType),
    StructField("year", IntegerType),
    StructField("weekTag", StringType)
  ))

  private val uniqueKeySchema = StructType(Seq(
    StructField("year", IntegerType),
    StructField("channel", StringType),
    StructField("division", StringType),
    StructField("gender", StringType),
    StructField("category", StringType)
  ))

  private val missingWeeksAggSchema = StructType(Seq(
    StructField("storeId", IntegerType),
    StructField("productId", StringType),
    StructField("year", IntegerType),
    StructField("weekTag", StringType),
    StructField("netSales", DoubleType),
    StructField("salesUnits", IntegerType)
  ))

  private val missingWeeksStoreSchema = StructType(Seq(
    StructField("storeId", IntegerType),
    StructField("channel", StringType)
  ))

  private val missingWeeksProductSchema = StructType(Seq(
    StructField("productId", StringType),
    StructField("division", StringType),
    StructField("gender", StringType),
    StructField("category", StringType)
  ))
}
