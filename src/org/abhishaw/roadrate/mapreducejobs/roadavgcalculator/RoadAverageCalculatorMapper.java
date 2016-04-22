package org.abhishaw.roadrate.mapreducejobs.roadavgcalculator;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class RoadAverageCalculatorMapper {
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

		@SuppressWarnings("deprecation")
		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			double sum = 0;
			NavigableMap<byte[], byte[]> nm = value.getFamilyMap(Bytes.toBytes("UserRating"));
			for (byte[] key : nm.keySet()) {
				count++;
				sum += Double.parseDouble(Bytes.toString(nm.get(key)));
			}
			if (count > 0)
				sum /= count;
			System.out.println(sum + ", " + count);
			Put put = new Put(row.get());
			put.add(Bytes.toBytes("Summary"), Bytes.toBytes("Average"), Bytes.toBytes(sum));
			context.write(row, put);
		}
	}
	/*
	 * 
	 * public static class MyTableReducer extends TableReducer<Text,
	 * DoubleWritable, ImmutableBytesWritable> {
	 * 
	 * @SuppressWarnings("deprecation") public void reduce(Text key,
	 * Iterable <DoubleWritable> values, Context context) throws IOException,
	 * InterruptedException { Put put = new Put(Bytes.toBytes(key.toString()));
	 * put.add(Bytes.toBytes("Summary"), Bytes.toBytes("Average"),
	 * Bytes.toBytes(values.iterator().next().get()));
	 * 
	 * context.write(null, put); } }
	 */}
