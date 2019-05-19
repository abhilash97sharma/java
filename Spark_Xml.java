package org.sql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Spark_Xml {
   public static void main(String[]args) {
	   SparkSession spark = SparkSession.builder().master("local").appName("Read_xml").getOrCreate();
/*	   StructType schema1 = DataTypes.createStructType(new StructField[] {
	            DataTypes.createStructField("Account",  DataTypes.StringType, true),
	            DataTypes.createStructField("Name", DataTypes.StringType, true),
	            DataTypes.createStructField("Number", DataTypes.StringType, true)
	    });*/
	   Dataset<Row> df = spark.read().format("xml").option("rowTag", "PubmedArticle").load("/home/abhilash/Downloads/pubmed19n0001.xml");
	   df.printSchema();
	   Dataset<Row> df1 = df.select("MedlineCitation.Article.ArticleTitle","MedlineCitation.ArticleIdList.ArticleId._VALUE");
	   df1.show(5,false);
   }
}
