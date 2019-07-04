package com.Spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import scala.collection.Seq;

public class Log_Analysis {
	static int count ;
  public static void main(String[]args) {
	  List<Integer> lst1 = new ArrayList<Integer>();
	  
	  SparkSession spark = new SparkSession.Builder().master("local").appName("Log_Analysis").getOrCreate();
	  Dataset<Row> df = spark.read().format("csv").option("delimiter", "|").option("header", true).option("inferSchema", true).load("/home/abhilash/Documents/files/airbnb_session_data.txt");
	  
	  Dataset<Row> df_new = df.withColumn("index", functions.row_number().over(Window.orderBy(functions.asc("ds"))));
	  
	  Dataset<Row> df_cpy = df_new;
	  
	  List<Row> lst = df_new.select(functions.col("did_search")).collectAsList();
	  Iterator itr = lst.iterator();
	  int val = Integer.parseInt((itr.next()+"").toCharArray()[1]+"");
      lst1.add(count);
	  
	  while(itr.hasNext()) {
		  int val2=Integer.parseInt((itr.next()+"").toCharArray()[1]+"");
		  System.out.println(val2);
		  if(val == val2) {
			  lst1.add(count);
		  }
		  else {
			  count++;
			  lst1.add(count);
		  }
		  val = val2;
	  }
	  
/*	  StructType schema1 = DataTypes.createStructType(new StructField[] {
			  DataTypes.createStructField("Pattern", DataTypes.IntegerType, true)
	  });*/	  		
	  
	  //
	  Dataset<Row> df_ind = spark.createDataset(lst1, Encoders.INT()).toDF();
	  Dataset<Row> df_ind1 = df_ind.withColumn("index", functions.row_number().over(Window.orderBy("value")));
	  
//    merging the df and df_ind
	  Dataset<Row> new_df = df_new.join(df_ind1, df_new.col("index").equalTo(df_ind1.col("index")), "inner");
	  
	  //converting a timestamp column into date type.
	  Dataset<Row> df1 = df.withColumn("ds", functions.to_date(functions.col("ds")));
	  
	  //converting a String format date into date type.
	  Dataset<Row> df2 = df1.withColumn("next_ds", functions.to_date(functions.col("next_ds")));
	  
	  //calculating a difference between the dates
	  Dataset<Row> df3 = df2.withColumn("Date_Diff", functions.datediff(functions.col("next_ds"), functions.col("ds")));
	  
	  //calculating the difference between the timestamp
//	  Dataset<Row> df4 = df3.withColumn("Min_timestamp", functions);
	  Dataset<Row> df5 = df3.withColumn("ts_min_sec", functions.unix_timestamp(functions.col("ts_min")));
//	  Dataset<Row> df6 = df5.select(functions.col("ts_min_sec"));
	  
	  WindowSpec window = Window.orderBy(functions.col("ds"));
	  Dataset<Row> df7 = df5.withColumn("ts_min_Lag", functions.lag(functions.col("ts_min"),1).over(window));
//	  Dataset<Row> df8 = df7.select(functions.col("did_search"));
	  
	  
//	  df6.show();
//	  System.out.println(lst1);
	  df_cpy.show(10, false);
	  df_new.show(10,false);
//	  df_new.printSchema();
	  
/*	  spark.udf().register("myFun", new UDF1<Integer,Integer>(){
		@Override
		public Integer call(Integer t1) throws Exception {
			// TODO Auto-generated method stub
			
			return 12;
		}
		  
	  },DataTypes.LongType);*/  
  }
}
