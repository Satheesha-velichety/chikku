
import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.sql.functions._



/**
  *
  * Singleton Class function library for generic XML parsing functions for processing of XMLs
  *
  */

object XmlParsingGenericFunctionLibrary {

  /**
    *
    *  function for etracting each column from  XML object and save it into table
    *
    * @param extractedOptionDf           Datafamr object of extracted XML object
    * @param fieldsMap                   Schema col name - Datatype map
    * @param path                        HDFS path for table storage
    * @param objectName                  XML Object name
    * @param objectPath                  XML xpath
    * @param source                      Data source name
    * @param dbName                      Database name
    * @param uniqueIdColName             Primary key columns
    *
    */

  def prepareAndInsertData(extractedOptionDf: Option[DataFrame],fieldsMap:LinkedHashMap[String,String],path:String,objectName:String,objectPath:String="AT_ROOT",source:String,dbName:String,UniQIdColName:LinkedHashMap[String,String]) {
    extractedOptionDf match {
      case Some(d) => {
        var extractedDf = d
        fieldsMap.foreach(field => extractedDf = extractColumn(field._1, s"${objectName}.${field._1}", field._2, extractedDf))
        fieldsMap.foreach(field =>println(s"${objectName}.${field._1}"))
        if(objectPath!="AT_ROOT") {
          extractedDf = extractedDf.withColumn("source", lit(objectPath))
        }
        extractedDf = extractedDf.drop(objectName)
        insertWithServiceFields_(extractedDf, dbName+"."+source+"_"+objectName,path,source)
      }
      case None => {
        println(s"Object ${objectName} is not present")
        var sourceColPresent = false
        if (objectPath != "AT_ROOT") {
          sourceColPresent = true
        }
        //XmlParsingGenericFunctionLibrary.createTable_(UniQIdColName,path, fieldsMap, dbName + "." + source + "_" + objectName, sourceColPresent)
      }
    }
  }

  /**
    *
    *  function for building structtype schema from map object
    *
    * @param schemaMap                   Schema col name - Datatype map
    * @return StructType                 Schema Object
    *
    */

  def buildSchema(schemaMap:scala.collection.mutable.LinkedHashMap[String,String]) : StructType = {
    def getDataType(dataTypeStr: String): DataType = {
      dataTypeStr match {
        case "Double" => {
          return DoubleType
        }
        case "Boolean" => {
          return BooleanType
        }
        case "Float" => {
          return FloatType
        }
        case "Long" => {
          return LongType
        }
        case "Integer" => {
          return IntegerType
        }
        case "Date" => {
          return DateType
        }
        case "Timestamp" => {
          return TimestampType
        }
        case "String" => {
          return StringType
        }
        case _ => {
          return StringType
        }
      }
    }

    import scala.collection.mutable.ArrayBuffer
    val strtyp:ArrayBuffer[StructField]= ArrayBuffer[StructField]()
    for ((key: String, value: String) <- schemaMap) {
      strtyp.+=(StructField(key, getDataType(value),true))
    }
    return StructType(strtyp.toArray)
  }

  /**
    *
    *  function for creating empty table
    *
    * @param schemaMap                   Schema col name - Datatype map
    * @param path                        HDFS path for table storage
    * @return StructType                 Schema Object
    *
    */
  def createEmptyTable(schema: StructType,path:String,tableName:String): Unit ={
    val spark=SparkSession.getActiveSession.get
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .write
      .options(Map("path"-> s"$path/$tableName"))
      .format("parquet")
      .mode(SaveMode.Append)
      .saveAsTable(tableName)
  }


  /**
    *
    *  function for creating empty table
    *
    * @param schemaMap                   Schema col name - Datatype map
    * @param path                        HDFS path for table storage
    * @return StructType                 Schema Object
    *
    */
  def createEmptyTable_(schema: StructType,path:String,tableName:String): Unit ={
    val spark=SparkSession.getActiveSession.get
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .write.partitionBy("imos_trans_dt")
      .options(Map("path"-> s"$path/$tableName"))
      .format("parquet")
      .mode(SaveMode.Append)
      .saveAsTable(tableName)
  }

  /**
    *
    *  function for creating a table
    *
    * @param fieldsMap                   Schema col name - Datatype map
    * @param path                        HDFS path for table storage
    * @param fieldsMap                   Field schema
    * @param tableName                   table name
    * @param sourceColPresent            boolean value representing wether source column present or not
    *
    */

  def createTable_(uniqValues:LinkedHashMap[String,String],path:String,fieldsMap:mutable.LinkedHashMap[String,String],tableName:String,sourceColPresent:Boolean=false): Unit ={
    val spark=SparkSession.getActiveSession.get

    val map=new scala.collection.mutable.LinkedHashMap[String,String]

    for(uniqValue <- uniqValues) {
      map.+=((uniqValue._1,uniqValue._2))
    }

    for(field <- fieldsMap){
      map.+=((field._1.replaceAll("._VALUE",""),field._2))
    }

    if(sourceColPresent){
      map.+=(("source","String"))
    }
    map.+=(("sequence_number","Double"))
    map.+=(("insertTimestamp","Timestamp"))
    map.+=(("XMLSource","String"))

    if(uniqValues.keySet.map(_.toLowerCase).contains("lastmodified")) {
      println("partitioned!!!")
      map.+=(("imos_trans_dt","String"))
      val schema=buildSchema(map)
      createEmptyTable_(schema,path,tableName)
    }else if (uniqValues.keySet.map(_.toLowerCase).contains("lastmodifieddate")){
      map.+=(("imos_trans_dt","String"))
      val schema=buildSchema(map)
      createEmptyTable_(schema,path,tableName)
    }else{
      println("Not partitioned!!!")
      val schema=buildSchema(map)
      createEmptyTable(schema,path,tableName)
    }
  }


  /**
    *
    *  function for saving a dataframe into a table by adding multiple fields
    *
    * @param df                          Dataframe object
    * @param tableName                   table name
    * @param source                      Data source name
    * @param sourceColPresent            boolean value representing wether source column present or not
    *
    */
  def insertWithServiceFields_(df: DataFrame, tableName: String,path:String,source:String,sourceColPresent:Boolean=false): Unit = {
    var modifiedDf = df
    modifiedDf = modifiedDf.withColumn("sequence_number", monotonically_increasing_id())
    modifiedDf = modifiedDf.withColumn("insertTimestamp", current_timestamp())
    modifiedDf = modifiedDf.withColumn("XMLSource", lit(source))

    modifiedDf.printSchema()
    if(modifiedDf.columns.map(_.toLowerCase).contains("lastmodified")) {
      println("partitioned!!!")
      modifiedDf = modifiedDf.withColumn("imos_trans_dt", col("lastModified").substr(0, 10))
      XmlParsingGenericFunctionLibrary.createEmptyTable_(modifiedDf.schema,path, tableName)
      modifiedDf.coalesce(4).write.partitionBy("imos_trans_dt").format("parquet").mode(SaveMode.Append).saveAsTable(tableName)
    }else if (modifiedDf.columns.map(_.toLowerCase).contains("lastmodifieddate")){
      println("partitioned!!!")
      modifiedDf = modifiedDf.withColumn("imos_trans_dt", col("lastModifiedDate").substr(0, 10))
      XmlParsingGenericFunctionLibrary.createEmptyTable_(modifiedDf.schema,path, tableName)
      modifiedDf.coalesce(4).write.partitionBy("imos_trans_dt").format("parquet").mode(SaveMode.Append).saveAsTable(tableName)
    }else{
      println("Not partitioned!!!")
      XmlParsingGenericFunctionLibrary.createEmptyTable(modifiedDf.schema,path, tableName)
      modifiedDf.coalesce(1).write.format("parquet").mode(SaveMode.Append).saveAsTable(tableName)
    }
  }


  /**
    *
    *  function for saving text to a file in HDFS
    *
    * @param filename           File name
    * @param text               text
    *
    */
  def writeObjectnameToFile(fileName:String,tableName:String)={
    val conf = new Configuration()
    val fs= FileSystem.get(conf)

    if(fs.exists(new Path(fileName))){
      println("File exists"+fileName)
      val output = fs.append(new Path(fileName))
      val writer = new PrintWriter(output)
      writer.append(tableName)
      writer.append("\n")
      writer.close()
    }else{
      println("File doesn't exists" + fileName)
      val output = fs.create(new Path(fileName))
      val writer = new PrintWriter(output)
      writer.append(tableName)
      writer.append("\n")
      writer.close()
    }
  }


  /**
    *
    *  function for extracting object from xml
    * @param Df                          whole XML dataframe
    * @param objectPath                  XML xpath
    * @param objectName                  XML Object name
    * @param uniqueIdColName             Primary key columns in array
    * @returns dataframe                 extracted dataframe
    */
  def extractContentsBuiltInObject(df: DataFrame, objectPath: String, objectName: String,uniqIdNames:Array[String]): Option[DataFrame] = {

    var specificObjectName = ""
    if (objectPath == objectName) {
      specificObjectName = objectName
    } else {
      specificObjectName = s"${objectPath}.${objectName}"
    }

    var objectInfoType: DataType = StringType
    try {
      objectInfoType = df.select(s"${specificObjectName}").schema.fields(0).dataType
    }
    catch {
      case e: Exception => {
        println(s"Object: ${objectName}, is not present. Extract failed")
        return None
      }
    }

    var modifiedDf = df
    objectInfoType match{
      case c: StringType => {
        println("Object  is not present")
        return None
      }
      case s: StructType => {
        val moreThanOneObjectPresent = modifiedDf.select(specificObjectName).schema.fields(0).dataType
        println(s"Object of present type: ${moreThanOneObjectPresent} is found")
        moreThanOneObjectPresent match{
          case a: StructType => {
            println(s"Only 1 object is present, the data will be deserialized as object")

            if(uniqIdNames.length == 1) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(specificObjectName).alias(objectName))
            }else if(uniqIdNames.length == 2){
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(uniqIdNames(1)).cast(StringType),col(specificObjectName).alias(objectName))
            }else if(uniqIdNames.length == 3){
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(uniqIdNames(1)).cast(StringType),col(uniqIdNames(2)).cast(StringType),col(specificObjectName).alias(objectName))
            }else if(uniqIdNames.length == 4){
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(uniqIdNames(1)).cast(StringType),col(uniqIdNames(2)).cast(StringType),col(uniqIdNames(3)).cast(StringType),col(specificObjectName).alias(objectName))
            }else if(uniqIdNames.length == 5){
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(uniqIdNames(1)).cast(StringType),col(uniqIdNames(2)).cast(StringType),col(uniqIdNames(3)).cast(StringType),col(uniqIdNames(4)).cast(StringType),col(specificObjectName).alias(objectName))
            }
            //modifiedDf.show()
            return Some(modifiedDf)
          }
          case array: ArrayType => {
            if(uniqIdNames.length == 1) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),explode(col(specificObjectName)).alias(objectName))
            }else if(uniqIdNames.length == 2){
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(uniqIdNames(1)).cast(StringType),explode(col(specificObjectName)).alias(objectName))
            }else if(uniqIdNames.length == 3) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType),col(uniqIdNames(2)).cast(StringType), explode(col(specificObjectName)).alias(objectName))
            }else if(uniqIdNames.length == 4) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType),col(uniqIdNames(2)).cast(StringType),col(uniqIdNames(3)).cast(StringType), explode(col(specificObjectName)).alias(objectName))
            }else if(uniqIdNames.length == 5) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType),col(uniqIdNames(2)).cast(StringType),col(uniqIdNames(3)).cast(StringType),col(uniqIdNames(4)).cast(StringType), explode(col(specificObjectName)).alias(objectName))
            }
            // modifiedDf.show()
            return Some(modifiedDf)
          }
        }
      }
      /**
      case array: ArrayType => {
        if(uniqIdNames.length == 1) {
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),explode(col(specificObjectName)).alias(objectName))
        }else if(uniqIdNames.length == 2){
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(uniqIdNames(1)).cast(StringType),explode(col(specificObjectName)).alias(objectName))
        } else if(uniqIdNames.length == 3){
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(uniqIdNames(1)).cast(StringType),col(uniqIdNames(2)).cast(StringType),explode(col(specificObjectName)).alias(objectName))
        }else if(uniqIdNames.length == 4){
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(uniqIdNames(1)).cast(StringType),col(uniqIdNames(2)).cast(StringType),col(uniqIdNames(3)).cast(StringType),explode(col(specificObjectName)).alias(objectName))
        }else if(uniqIdNames.length == 5){
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType),col(uniqIdNames(1)).cast(StringType),col(uniqIdNames(2)).cast(StringType),col(uniqIdNames(3)).cast(StringType),col(uniqIdNames(4)).cast(StringType),explode(col(specificObjectName)).alias(objectName))
        }
        //modifiedDf.show()
        return Some(modifiedDf)
      }
        **/
      case array: ArrayType => {

        if (uniqIdNames.length == 1) {
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), explode(col(specificObjectName)).alias(objectName))
        } else if (uniqIdNames.length == 2) {
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), explode(col(specificObjectName)).alias(objectName))
        } else if (uniqIdNames.length == 3) {
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(uniqIdNames(2)).cast(StringType), explode(col(specificObjectName)).alias(objectName))
        } else if (uniqIdNames.length == 4) {
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(uniqIdNames(2)).cast(StringType), col(uniqIdNames(3)).cast(StringType), explode(col(specificObjectName)).alias(objectName))
        } else if (uniqIdNames.length == 5) {
          modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(uniqIdNames(2)).cast(StringType), col(uniqIdNames(3)).cast(StringType), col(uniqIdNames(4)).cast(StringType), explode(col(specificObjectName)).alias(objectName))
        }

        val moreThanOneObjectPresent = modifiedDf.select(col(objectName)).schema.fields(0).dataType
        moreThanOneObjectPresent match {
          case a: StructType => {
            if (uniqIdNames.length == 1) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(objectName).alias(objectName))
            } else if (uniqIdNames.length == 2) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(objectName).alias(objectName))
            } else if (uniqIdNames.length == 3) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(uniqIdNames(2)).cast(StringType), col(objectName).alias(objectName))
            } else if (uniqIdNames.length == 4) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(uniqIdNames(2)).cast(StringType), col(uniqIdNames(3)).cast(StringType), col(objectName).alias(objectName))
            } else if (uniqIdNames.length == 5) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(uniqIdNames(2)).cast(StringType), col(uniqIdNames(3)).cast(StringType), col(uniqIdNames(4)).cast(StringType), col(objectName).alias(objectName))
            }
            modifiedDf.cache()
            return Some(modifiedDf)
          }
          case array: ArrayType => {
            if (uniqIdNames.length == 1) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), explode(col(objectName)).alias(objectName))
            } else if (uniqIdNames.length == 2) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), explode(col(objectName)).alias(objectName))
            } else if (uniqIdNames.length == 3) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(uniqIdNames(2)).cast(StringType), explode(col(objectName)).alias(objectName))
            } else if (uniqIdNames.length == 4) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(uniqIdNames(2)).cast(StringType), col(uniqIdNames(3)).cast(StringType), explode(col(objectName)).alias(objectName))
            } else if (uniqIdNames.length == 5) {
              modifiedDf = modifiedDf.select(col(uniqIdNames(0)).cast(StringType), col(uniqIdNames(1)).cast(StringType), col(uniqIdNames(2)).cast(StringType), col(uniqIdNames(3)).cast(StringType), col(uniqIdNames(4)).cast(StringType), explode(col(objectName)).alias(objectName))
            }
            modifiedDf.cache()
            return Some(modifiedDf)
          }
        }
      }
      case _ => {
        println("No match was made")
        println(s"Object info type is: ${objectInfoType}")
        return Some(modifiedDf)
      }
    }
  }


  /**
    *
    *  function for etracting each column from  XML object and save it into table
    *
    * @param columnNameRaw                Raw column name
    * @param extractColumnName            Extracted column name
    * @param columnType                   Column type
    * @param df                           Dataframe
    * @returns dataframe                  dataframe
    *
    */
  def extractColumn(columnNameRaw: String, extractColumnName: String, columnType: String, df: org.apache.spark.sql.DataFrame,customObjectMap:LinkedHashMap[String,String]=LinkedHashMap()): DataFrame = {
    val sparkSession=SparkSession.getActiveSession.get
    import sparkSession.implicits._

    var processedDf = df
    var columnName = columnNameRaw

    if(columnNameRaw.contains("._VALUE")){
      columnName = columnNameRaw.replace("._VALUE", "")
    }

    val rawColumnName = columnName + "Raw"
    try {
      columnType match {
        case "Double" | "double" => {
          try {
            processedDf = processedDf.withColumn(rawColumnName, $"$extractColumnName")

            if(processedDf.select(rawColumnName).schema.head.dataType.typeName.contains("struct")) {
              processedDf = processedDf.withColumn(columnName, col(rawColumnName+"._VALUE").cast(DoubleType))
            }else{
              processedDf = processedDf.withColumn(columnName, col(rawColumnName).cast(DoubleType))
            }
            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              processedDf = processedDf.drop(rawColumnName)
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
              processedDf = processedDf.withColumn(columnName, lit(0).cast(DoubleType))
            }
          }
        }
        case "Boolean" | "boolean" => {
          try {
            processedDf = processedDf.withColumn(rawColumnName, $"$extractColumnName")
            if(processedDf.select(rawColumnName).schema.head.dataType.typeName.contains("struct")) {
              processedDf = processedDf.withColumn(columnName, col(rawColumnName+"._VALUE").cast(BooleanType))
            }else{
              processedDf = processedDf.withColumn(columnName, col(rawColumnName).cast(BooleanType))
            }

            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              processedDf = processedDf.drop(rawColumnName)
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
              processedDf = processedDf.withColumn(columnName, lit("").cast(BooleanType))
            }
          }
        }
        case "Float" | "float" => {
          try{
            processedDf = processedDf.withColumn(rawColumnName, $"$extractColumnName")
            if(processedDf.select(rawColumnName).schema.head.dataType.typeName.contains("struct")) {
              processedDf = processedDf.withColumn(columnName, col(rawColumnName+"._VALUE").cast(FloatType))
            }else{
              processedDf = processedDf.withColumn(columnName, col(rawColumnName).cast(FloatType))
            }
            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              processedDf = processedDf.drop(rawColumnName)
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
              processedDf = processedDf.withColumn(columnName, lit(0).cast(FloatType))
            }
          }
        }
        case "Integer" | "integer" => {
          try{
            processedDf = processedDf.withColumn(rawColumnName, $"$extractColumnName")
            println(s"Data type of $extractColumnName is "+ processedDf.select(rawColumnName).schema.head.dataType.typeName)
            val datatype=processedDf.select(rawColumnName).schema.head.dataType.typeName
            if(processedDf.select(rawColumnName).schema.head.dataType.typeName.contains("struct")) {
              processedDf = processedDf.withColumn(columnName, col(rawColumnName+"._VALUE").cast(IntegerType))
            }else{
              processedDf = processedDf.withColumn(columnName, col(rawColumnName).cast(IntegerType))
            }
            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              processedDf = processedDf.drop(rawColumnName)
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
              processedDf = processedDf.withColumn(columnName, lit(0).cast(IntegerType))
            }
          }
        }
        case "Date" | "date" => {
          try{
            processedDf = processedDf.withColumn(rawColumnName, $"$extractColumnName")

            if(processedDf.select(rawColumnName).schema.head.dataType.typeName.contains("struct")) {
              processedDf = processedDf.withColumn(columnName, col(rawColumnName+"._VALUE").cast(DateType))
            }else{
              processedDf = processedDf.withColumn(columnName, col(rawColumnName).cast(DateType))
            }

            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              processedDf = processedDf.drop(rawColumnName)
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
              processedDf = processedDf.withColumn(columnName, lit("").cast(DateType))
            }
          }
        }
        case "Timestamp" | "timestamp" => {
          try{
            processedDf = processedDf.withColumn(rawColumnName, $"$extractColumnName")

            if(processedDf.select(rawColumnName).schema.head.dataType.typeName.contains("struct")) {
              processedDf = processedDf.withColumn(columnName, col(rawColumnName+"._VALUE").cast(TimestampType))
            }else{
              processedDf = processedDf.withColumn(columnName, col(rawColumnName).cast(TimestampType))
            }
            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              processedDf = processedDf.drop(rawColumnName)
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
              processedDf = processedDf.withColumn(columnName, lit("").cast(TimestampType))
            }
          }
        }
        case "CUSTOM" => {
          try{
            processedDf = processedDf.withColumn(rawColumnName, $"$extractColumnName")

            for ((colName,colType) <- customObjectMap) {
              processedDf = processedDf.withColumn(colName, col(s"$rawColumnName.$colName").cast(colType))
            }
            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              processedDf = processedDf.drop(rawColumnName)
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
            }
          }
        }case "String" | "string" => {
          try{

            processedDf = processedDf.withColumn(rawColumnName, $"$extractColumnName")
            println(s"Data type of $extractColumnName is "+ processedDf.select(rawColumnName).schema.head.dataType.typeName)
            val datatype=processedDf.select(rawColumnName).schema.head.dataType.typeName
            if(datatype.toLowerCase().contains("struct")) {
              processedDf = processedDf.withColumn(columnName, col(rawColumnName+"._VALUE").cast(StringType))
            }else{
              processedDf = processedDf.withColumn(columnName, col(rawColumnName).cast(StringType))
            }
            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              processedDf = processedDf.drop(rawColumnName)
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
              processedDf = processedDf.withColumn(columnName, lit("").cast(StringType))
            }
          }
        }case "Long" => {
          try{

            processedDf = processedDf.withColumn(rawColumnName, $"$extractColumnName")
            val datatype=processedDf.select(rawColumnName).schema.head.dataType.typeName
            if(datatype.toLowerCase().contains("struct")) {
              processedDf = processedDf.withColumn(columnName, col(rawColumnName+"._VALUE").cast(LongType))
            }else{
              processedDf = processedDf.withColumn(columnName, col(rawColumnName).cast(LongType))
            }
            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              processedDf = processedDf.drop(rawColumnName)
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
              processedDf = processedDf.withColumn(columnName, lit(0).cast(LongType))
            }
          }
        }
        case _ => {
          try{
            processedDf = processedDf.withColumn(columnName, $"$extractColumnName")
            processedDf = processedDf.drop(rawColumnName)
          }
          catch {
            case e: Exception => {
              println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
            }
          }
        }

      }
    }
    catch {
      case e: Exception => {
        println(s"Extraction failed for column: $columnName, of data type: $columnType , extracted from: $extractColumnName")
        throw e
      }
    }
    return processedDf
  }
}
