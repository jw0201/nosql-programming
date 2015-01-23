package com.aaa.mapred;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, FloatWritable> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		float sum = 0;
		int count = 0;
       
    // loop�� ���� ���� ���Ѵ�.
		for (Text value : values) {
			String[] columns = value.toString().trim().split(",");
			
			if(columns != null && columns.length == 2 && !columns[1].equals(".")) {
				count++;
				sum += Float.parseFloat(columns[1]);
			}
		}
		// ��հ��� ����Ѵ�.
		context.write(key, new FloatWritable(sum/count));
	}
}
