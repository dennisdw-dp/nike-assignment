package me.assignment.data
import org.apache.spark.sql.DataFrame
import me.assignment.AssignmentJob.spark

private[assignment] class LocalAssignmentData(basePath: String,
                          sales: String = "sales.csv",
                          calendar: String = "calendar.csv",
                          store: String = "store.csv",
                          product: String = "product.csv") extends AssignmentData {

  private def read(file: String, alias: String): DataFrame =
    spark.read
      .option("header", value = true)
      .csv(s"$basePath/$file")
      .alias(alias)

  override lazy val salesDF: DataFrame = read(sales, "sales")
  override lazy val calendarDF: DataFrame = read(calendar, "calendar")
  override lazy val storeDF: DataFrame = read(store, "store")
  override lazy val productDF: DataFrame = read(product, "product")

}
