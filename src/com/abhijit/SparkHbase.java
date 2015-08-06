package com.abhijit;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SparkHbase {

	public static void main(String[] args) {

		try {
			SparkConf sconf = new SparkConf().setAppName("App").setMaster("local[4]");
			JavaSparkContext sc = new JavaSparkContext(sconf); 
			Configuration conf = HBaseConfiguration.create();

			System.out.println("---> zookeeper.znode.parent = " + conf.get("zookeeper.znode.parent"));
			System.out.println("---> hbase.zookeeper.quorum = " + conf.get("hbase.zookeeper.quorum"));
			System.out.println("---> HBaseConfiguration = " + conf);
			
			conf.set("hbase.master", "devhdp01");
			conf.setInt("timeout", 120000);
			conf.set("hbase.zookeeper.quorum", "devhdp01");
			//conf.set("zookeeper.znode.parent","/hbase");
			conf.set(TableInputFormat.INPUT_TABLE, "ambarismoketest");

			JavaPairRDD<ImmutableBytesWritable, Result> vendorRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,org.apache.hadoop.hbase.client.Result.class);
					
			System.out.println("Number of Records found : " + vendorRDD.count());
			
			//JavaPairRDD<String, Row> rowPairRDD = vendorRDD .mapToPair(f);
			// Generate the schema based on the string of schema
			final List<StructField> keyFields = new ArrayList<StructField>();
			for (String fieldName: getSchemaString().split(",")) {
			     KeyFields.add(DataType.createStructField(fieldName,
			DataType.StringType, true));
			}
			StructType schema = DataType.createStructType(keyFields);
			JavaRDD<Row> rowRDD = vendorRDD.map(
			     new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
			    	 public Row call(Tuple2<ImmutableBytesWritable, Result> re)
								throws Exception {
					    return createRow(re, this.getSchemaString());
					}
			});
							
			SQLContext sqlContext = new SQLContext(sc);
			// Apply the schema to the RDD.
			JavaSchemaRDD schemaRDD = sqlContext.applySchema(rowRDD, schema);
			schemaRDD.registerTempTable("queryEntity");
			JavaSchemaRDD retRDD = sqlContext.sql("SELECT * FROM mldata WHERE name='Spark'");
			conf.set(TableInputFormat.INPUT_TABLE, "customer1");
			JavaPairRDD<ImmutableBytesWritable, Result> customerRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,org.apache.hadoop.hbase.client.Result.class);
			
			System.out.println("Number of Records found : " + customerRDD.count());
			sc.stop();
			sc.close();
		}  catch (Exception e) {
			e.printStackTrace();
		}
	}

}
