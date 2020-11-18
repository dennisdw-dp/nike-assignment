package me.assignment

import io.circe
import io.circe.parser
import me.assignment.conf.AssignmentParameters._
import me.assignment.conf.{AssignmentParameters, Weekly, Yearly}
import me.assignment.data.{CalendarData, ProductData, SalesData, StoreData, WeekTags}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.DataFrameNaFunctions

import scala.io.Source

class AssignmentJob(private val params: AssignmentParameters) {

  private val inputData = params.input.getData
  private val sales = new SalesData(inputData.salesDF)
  private val calendar = new CalendarData(inputData.calendarDF)
  private val store = new StoreData(inputData.storeDF)
  private val product = new ProductData(inputData.productDF)

  private def filterSelectedDivisions(sales: DataFrame, product: DataFrame): DataFrame = {
    if (params.divisions.isEmpty) {
      // If no divisions are selected, we instead aggregateWeekly all of them, which means no filtering takes place here
      return sales
    }

    // Filter the products to select only those in the requested divisions
    val matchingProducts = product.filter(col("division").isin(params.divisions: _*))

    // Perform a semi join to retain only the sales data for the correct set of products
    sales.join(matchingProducts, Seq("productId"), "semi")
  }

  private def aggregateStores(sales: DataFrame, calendar: DataFrame): DataFrame = {
    // Join with the calendar to get the week numbers
    val salesWithWeekNumber = sales.join(calendar, "dateId")

    // Select the group-by columns based on the selected granularity
    val grouped = params.granularity match {
      case Weekly => salesWithWeekNumber.groupBy(col("storeId"), col("year"), col("weekTag"), col("productId"))
      case Yearly => salesWithWeekNumber.groupBy(col("storeId"), col("year"), col("productId")) // TODO
    }

    // Calculate the sums of the two columns we are interested in
    val aggregated = grouped.agg(
      "netSales" -> "sum",
      "salesUnits" -> "sum"
    )

    // Make the column names a little prettier
    aggregated
      .withColumnRenamed("sum(netSales)", "netSales")
      .withColumnRenamed("sum(salesUnits)", "salesUnits")
  }

  private def addMissingWeeks(aggregated: DataFrame, store: DataFrame, product: DataFrame): DataFrame = {
    // Join with store and product to get the readable values for division, gender etc.
    val joined = aggregated
      .join(store, "storeId")
      .join(product, "productId")

    // Construct and add the unique key
    val withUniqueKey = joined.withColumn("uniqueKey", makeUniqueKey(joined))

    // Cross join with the week tags (W1-W52) so there is a row for every possible week, not just every week with sales
    val allWeeksCrossJoined = withUniqueKey.drop("weekTag", "netSales", "salesUnits").distinct().crossJoin(WeekTags.weekTagDF)

    // Join back to the main dataframe and replace nulls (= no sales that week) with 0's
    val withAllWeeks = allWeeksCrossJoined.join(
      withUniqueKey.select("uniqueKey", "year", "weekTag", "netSales", "salesUnits"),
      Seq("uniqueKey", "year", "weekTag"),
      "left_outer")
    withAllWeeks.na.fill(0)
  }

  private def assembleFinalWeeklyData(dataFrame: DataFrame): DataFrame = {
    // Create the dataframes which will be nested into the final output (the elements of the "dataRows" field)
    val netSalesDataRows = makeDataRows(dataFrame, "netSales", "Net Sales")
    val salesUnitsDataRows = makeDataRows(dataFrame, "salesUnits", "Sales Units")

    // The week-level data is going to be contained in the dataRows field, so for this 'outer' dataframe we no longer need it
    val dataPerYear = dataFrame.select("uniqueKey", "division", "gender", "category", "channel", "year").distinct()

    // Join with the two dataRows.
    // They will be in separate columns for now, and will be merged into a single array in the next step.
    val withDataRows = dataPerYear
      .join(netSalesDataRows, Seq("uniqueKey", "year"))
      .join(salesUnitsDataRows, Seq("uniqueKey", "year"))

    // Merge the two dataRows elements into an array
    val withDataRowsInStructs = withDataRows
      .withColumn("dataRows", array("Net Sales", "Sales Units"))
      .drop("Net Sales", "Sales Units")

    // Partition by the unique key to get one output file per key
    withDataRowsInStructs.repartition(col("uniqueKey"))
  }

  private def makeDataRows(dataFrame: DataFrame, valueColumn: String, outputName: String): DataFrame = {
    // Pivot by the week tags, so we can then create a struct out of them
    val pivoted = dataFrame.groupBy("uniqueKey", "year").pivot("weekTag").sum(valueColumn)

    // Get the columns corresponding to the week tags created by the pivoting.
    val pivotCols = pivoted.columns
      .filter(_.matches("W\\d\\d?"))
      .sortBy(_.tail.toInt) // To ensure the columns are ordered by week number
      .map(col)

    // Construct the struct:
    // {
    //   "dataRowId":  "...",
    //   "dataRow": {
    //     "W1": ...
    //     ...
    //   }
    // }
    // The cast is to set the correct column names.
    pivoted
      .withColumn(outputName, struct(lit(outputName), struct(pivotCols: _*)).cast(Schemas.dataRowsSchema))
      // Then drop all the week tag columns
      .select("uniqueKey", "year", outputName)
  }

  private def assembleFinalYearlyData(dataFrame: DataFrame, store: DataFrame, product: DataFrame): DataFrame = {
    // Join with store and product to get the readable values for division, gender etc.
    val joined = dataFrame
      .join(store, "storeId")
      .join(product, "productId")

    // Construct and add the unique key
    val withUniqueKey = joined.withColumn("uniqueKey", makeUniqueKey(joined))

    // For year-based results, simply group by year and sum the relevant numbers
    val aggregated = withUniqueKey.groupBy(
      col("uniqueKey"),
      col("division"),
      col("gender"),
      col("category"),
      col("channel"),
      col("year")
    ).agg(
      "netSales" -> "sum",
      "salesUnits" -> "sum"
    )

    // Rename the aggregated columns for readability
    aggregated
      .withColumnRenamed("sum(netSales)", "Net Sales")
      .withColumnRenamed("sum(salesUnits)", "Sales (Units)")
  }

  private def makeUniqueKey(dataFrame: DataFrame): Column = {
    concat_ws("_",
      dataFrame("year"),
      upper(dataFrame("channel")),
      upper(dataFrame("division")),
      upper(dataFrame("gender")),
      upper(dataFrame("category"))
    )
  }

  /**
   * Perform the transformation as specified in the parameters, and send the results to the configured output.
   */
  def execute(): Unit = {
    // First, filter out any unwanted data based on divisions listed in the input file
    val filteredSales = filterSelectedDivisions(sales.dataFrame, product.dataFrame)

    // Aggregate over the stores to get numbers per product per week or year (based on granularity)
    val aggregated = aggregateStores(filteredSales, calendar.dataFrame)

    val out = params.granularity match {
      case Weekly =>
        // Add rows with zero netSales/salesUnits for weeks without sales, instead of having them missing
        val allWeeks = addMissingWeeks(aggregated, store.dataFrame, product.dataFrame)

        // Put the data in the correct output format
        assembleFinalWeeklyData(allWeeks)
      case Yearly =>
        // Calculate the unique key and put the data in the correct output format
        assembleFinalYearlyData(aggregated, store.dataFrame, product.dataFrame)
    }

    params.output.processResults(out)
  }
}

object AssignmentJob {

  // private[assignment] val spark: SparkSession = SparkSession.builder().master("local[8]").getOrCreate()
  private[assignment] val spark: SparkSession = SparkSession.builder().getOrCreate()

  /**
   * Create an AssignmentJob based on a specification in json.
   * @param jobSpec The job specification in json format, which should be decodable to an instance of AssignmentParameters
   * @return Either the job if it was decoded successfully, or a ParsingFailure or DecodingFailure otherwise.
   */
  def create(jobSpec: String): Either[circe.Error, AssignmentJob] = {
    // Parse the json into circe's format
    val json = parser.parse(jobSpec) match {
      case Left(failure) => return Left(failure)
      case Right(value) => value
    }

    // Have circe decode the json to a parameters object
    val parameters = json.as[AssignmentParameters] match {
      case Left(failure) => return Left(failure)
      case Right(value) => value
    }

    Right(new AssignmentJob(parameters))
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      System.err.println("A path to the file containing the job specification is required")
    }

    val source = Source.fromFile(args.head)
    try {
      val content = source.mkString

      val job = create(content) match {
        case Left(failure) => throw new IllegalArgumentException(s"Failed to parse the input file", failure)
        case Right(value) => value
      }

      job.execute()
    } finally {
      source.close()
    }
  }
}