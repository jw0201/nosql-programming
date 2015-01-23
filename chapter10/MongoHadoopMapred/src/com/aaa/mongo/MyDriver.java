package com.aaa.mongo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;

public class MyDriver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// MongoDB�� ����� ��θ� Configuration ��ü�� �����Ѵ�.
		conf.set("mongo.input.uri", "mongodb://master:27017/INTLCTRY.data");
		conf.set("mongo.output.uri", "mongodb://master:27017/INTLCTRY.data");
		Job job = Job.getInstance(conf, "Map Reduce in MongoDB");
		
		job.setOutputKeyClass(BSONWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
 
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class); 
 
 		// ����� ������ MongoInputFormat.class�� MongoOutputFormat.class��
    // �����Ѵ�. map�� reduce �Լ��� �����͸� document�� ó���Ѵ�.
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);
 
		job.setJarByClass(MyDriver.class);
 
		job.submit();
	}

}
