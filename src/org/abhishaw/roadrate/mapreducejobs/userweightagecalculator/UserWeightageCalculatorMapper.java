package org.abhishaw.roadrate.mapreducejobs.userweightagecalculator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;

import org.abhishaw.roadrate.dao.reader.RoadReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class UserWeightageCalculatorMapper {
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

		@SuppressWarnings("deprecation")
		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			List<Double> list = new ArrayList<Double>();
			NavigableMap<byte[], byte[]> nm = value.getFamilyMap(Bytes.toBytes("RoadRating"));
			System.out.print(Bytes.toString(row.get())+" [ ");
			for (byte[] key: nm.keySet()){
				list.add(new Double(100-getReductionPercentage(key, nm.get(key))));
				System.out.println("{ "+Bytes.toString(key) +":" +Bytes.toString(nm.get(key))+ "},");
			}
			Collections.sort(list);
			System.out.println("]\n"+list.toString());
			double weightage=10;
			for (Double decPer:list){
				weightage*=decPer/100;
			}
			System.out.println("Wieghtage: "+weightage);
			Put put = new Put(row.get());
			put.add(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Weightage"), Bytes.toBytes(weightage));
			context.write(row, put);
		}

		private Double getReductionPercentage(byte[] key, byte[] bs) throws IOException {
			Get get = new Get(key);
			get.addColumn(Bytes.toBytes("Summary"), Bytes.toBytes("Average"));
			Result result = RoadReader.getRoad(get);
			
			return Math.abs(Double.parseDouble(Bytes.toString(bs)) - Bytes.toDouble(result.getValue(Bytes.toBytes("Summary"), Bytes.toBytes("Average"))));
		}
	}

}
