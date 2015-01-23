package com.aaa.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoin extends Reducer<Text, Text, CountryData, Text> {

	private CountryData outputKey = new CountryData();
	private Text outputValue = new Text();
	
	// reduce �Լ��� Text key�� ���� MapperINTL������ MapperOECD����
  // ������ ���� �Էµȴ�. �׷��Ƿ� Reducer���� Join �۾��� �� �� �ִ�.
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       
		for (Text value : values) {
			String[] columns = value.toString().trim().split(",");
			
			outputKey.setCountry(columns[0]);
			outputKey.setTitle(columns[1]);
			outputKey.setAge(columns[2]);
			outputKey.setSex(columns[3]);
			outputKey.setUnits(columns[4]);
			outputKey.setFrequency(columns[5].charAt(0));
			outputKey.setSeasonalAdjust(columns[6]);
			outputKey.setUpdateDate(columns[7]);
			
			outputValue.set(","+columns[8]+","+columns[9]);
			
			context.write(outputKey, outputValue);
		}
	}
}
