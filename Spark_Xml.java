package org.sql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.api.java.UDF2;

public class Spark_Xml {
   public static void main(String[]args) {
	   SparkSession spark = SparkSession.builder().master("local").appName("Read_xml").getOrCreate();
/*	   StructType schema1 = DataTypes.createStructType(new StructField[] {
	            DataTypes.createStructField("Account",  DataTypes.StringType, true),
	            DataTypes.createStructField("Name", DataTypes.StringType, true),
	            DataTypes.createStructField("Number", DataTypes.StringType, true)
	    });*/
	   
	   // creating udf 
/*	   spark.udf().register("fun12", new UDF2<String,String,String>(){
	    	 public String call(String[] sd1, String[] sd2) throws Exception{
	    		 return sd1+sd2;
	    	 }

	   },DataTypes.StringType);*/
	   
	   Dataset<Row> df = spark.read().format("xml").option("rowTag", "PubmedArticle").load("/home/abhilash/Downloads/pubmed19n0112.xml");
	   Dataset<Row> df1 = df.select("MedlineCitation.MeshHeadingList.MeshHeading.DescriptorName","MedlineCitation.Article.Abstract.AbstractText._VALUE","MedlineCitation.Article.ArticleTitle","MedlineCitation.Article.AuthorList.Author.ForeName","MedlineCitation.Article.AuthorList.Author.LastName");
	   Dataset<Row> df4 = df1.withColumn("Desc", functions.explode(functions.col("DescriptorName"))).drop(functions.col("DescriptorName"));
	   Dataset<Row> df6 = df4.withColumn("Desc1", functions.col("Desc").cast(DataTypes.StringType));
       Dataset<Row> df5 = df6.withColumn("Type", functions.split(functions.col("Desc1"), ",").getItem(0));
       Dataset<Row> df7 = df5.withColumn("Code", functions.split(functions.col("Desc1"), ",").getItem(2));
       Dataset<Row> df8 = df7.withColumn("Content", functions.split(functions.col("Desc1"), ",").getItem(3)).drop(functions.col("Desc1"));
	   Dataset<Row> df9 = df8.withColumn("Type", functions.regexp_replace(functions.col("Type"), "\\[", ""));
	   Dataset<Row> df10 = df9.filter(functions.col("Type").equalTo("Y")).drop(functions.col("Desc"));
	   Dataset<Row> df11 = df10.withColumnRenamed("_VALUE", "Abstract_Text");
//	   Dataset<Row> df12 = df11.select(functions.callUDF("fun12", functions.col("ForeName"),functions.col("LastName")));
//       df12.show(5,false);
	   df11.printSchema();
   }
}
