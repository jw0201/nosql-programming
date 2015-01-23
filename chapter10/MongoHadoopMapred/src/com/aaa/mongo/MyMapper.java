package com.aaa.mongo;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;

public class MyMapper extends Mapper<Object, BasicDBObject, BSONWritable, DoubleWritable> {
	
	// inputFormatClass�� MongoInputFormat.class�� �����ϸ� �Ʒ��� ����
  // map�Լ��� <Key,Value>�� <Object, BasicDBObject>�� ������ ���´�.
  // Value��ü�� Document���� ��� �ִ�.
	@Override
	public void map(Object key, BasicDBObject value, Context context) throws IOException, InterruptedException{
		
		double outputValue = 0.0;
		BasicDBObjectBuilder outputKeyBuilder = BasicDBObjectBuilder.start();
		
		// �� Key�� ���� Value ���� �о�´�.
		String bCountry = (String)value.get("country");
		String bTitle = (String)value.get("title");
		String bAgeRange = (String)value.get("ageRange");
		String bSex = (String)value.get("sex");
		String bUnit = (String)value.get("unit");
		char bFreq = ((String)value.get("freq")).charAt(0);
		String bSeasonalAdj = (String)value.get("seasonalAdj");
		Date bUpdateDate = (Date)value.get("updateDate");
		
		// BasicDBList�κ��� ������ ������ document value�� Key���� �����.
		outputKeyBuilder.append("country", bCountry).append("title", bTitle).append("ageRange", bAgeRange).append("sex", bSex)
					.append("unit", bUnit).append("freq", bFreq).append("seasonalAdj", bSeasonalAdj).append("updateDate", bUpdateDate);
		
		// BasicDBList Value �κ��� �о��	 Value ���� �����.
		BasicDBList valueDataList = (BasicDBList)value.get("countryData");
		
		for(int index=0 ; index<valueDataList.size(); index++) {
			BasicDBObject tmpObj = (BasicDBObject)valueDataList.get(index);
			outputValue = tmpObj.getDouble("value");
	    // <Key,Value>�� ����Ѵ�.           
			context.write(new BSONWritable(outputKeyBuilder.get()), new DoubleWritable(outputValue));
		}
	}
}
