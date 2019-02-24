package models
import java.sql.Timestamp

import javax.inject.Inject
import org.apache.spark.sql._


class SparkConnInit @Inject() (sparkConfigIntitaion: SparkConfigIntitaion) {
  case class ConsolidateSales(Sales_Location_Id: Int,
                              Section_Id: Int,
                              Bill_No: String,
                              Bill_Date: Timestamp,
                              Item_Serial_No: Int,
                              Bill_Number: String,
                              Hand_Bill: Boolean,
                              Bulk_Bill: Boolean,
                              Param_Bill: Boolean,
                              Ecom_Bill: Boolean,
                              Payment_Id: String,
                              Payment_Date: Timestamp,
                              Bill_Entry_Date: Timestamp,
                              Company_Section_Id: Int,
                              Defined_Section_Id: Int,
                              Customer_Id: String,
                              Bill_Total_Amount: Double,
                              Bill_Discount_Percentage: Double,
                              Bill_Discount_Amount: Double,
                              Bill_Net_Amount: Double,
                              Inv_Byno: String,
                              Byno_Serial: String,
                              Byno_Prod_Serial: String,
                              Byno: String,
                              Product_Code: String,
                              Billing_Product_Group_Id: String,
                              Defined_Product_Code: String,
                              Counter_Id: Int,
                              Defined_Counter_Id: Int,
                              Item_Rate: Double,
                              Item_Actual_Rate: Double,
                              Item_Qty: Double,
                              Item_Pieces: Double,
                              Item_Quantity: Double,
                              Item_Qty_Discount: Double,
                              Item_Discount_Percentage: Double,
                              Item_Discount_Amount: Double,
                              Salesman_Code: Int,
                              Location_Id: Int,
                              Suspense_Issue_Id: Int,
                              Suspense_Issue_SerialNo: Int,
                              Ecom: Boolean,
                              Ecom_Code: String,
                              Sales_Quantity: Double,
                              Sales_Value: Double,
                              Last_Updated_Date_Time: Timestamp,
                              Section: Int,
                              Level1: String,
                              Level2: String,
                              Level3: String,
                              Item_Cost_Price: Double,
                              Bill_Reference_No: String,
                              GST_Tax: Double,
                              SP_WO_Tax: Double,
                              HSN_Code: String,
                              Discount_Value: Double)

case class FastMoveProducts(Product_Code: String,Product_Description: String)

  def loadConsolidateSales(tname: DataFrame) = {
    val encoder1 = Encoders.product[ConsolidateSales]
    val consolidatesalesDS =tname.as(encoder1)
    //val consolidatesales = "/user/rmkv/hive/ConsolidationSales/we"
    //val consolidatesalesDS = spark.read.csv(consolidatesales).as[ConsolidateSales]
    consolidatesalesDS
  }


  def loadFastMoveProductsDS(tname: DataFrame) = {
    val encoder = Encoders.product[FastMoveProducts]
    val fastmoveproductsDS =tname.as(encoder)
    //import spark.implicits._
   // val fmpschema = StructType(Array(StructField("Product_Code",StringType),StructField("Product_Description",StringType)))
    //val fastmoveproducts = "/user/hive/FastMoveProducts/part-m-00000"
    //val fastmoveproductsDS = spark.read.schema(fmpschema).option("delimiter", "\001").csv(fastmoveproducts).as[FastMoveProducts]
    fastmoveproductsDS
  }


  def productAnalysis(cssDS: DataFrame,fmpDS: DataFrame): DataFrame =  {
  cssDS.createOrReplaceTempView("Consolidated_Sales")
  fmpDS.createOrReplaceTempView("Fast_Move_Products")
    import sparkConfigIntitaion.sparkSession.implicits._
    val ds = Seq(1).toDF("Sales_Location_Id")
    ds.createOrReplaceTempView("Saleslocation")


    //val saleslocation = sparkConfigIntitaion.sparkSession.
    val query =
      "select * from Consolidated_Sales s Join Saleslocation on s.Sales_Location_Id=Saleslocation.Sales_Location_Id Where Bill_Date Between '2018-10-19' and '2018-10-19' and SUBSTRING(Bill_No,1,2)='SO' limit 2"
  val queryRS: DataFrame = sparkConfigIntitaion.sparkSession.sql(query)
  queryRS
  }






}
