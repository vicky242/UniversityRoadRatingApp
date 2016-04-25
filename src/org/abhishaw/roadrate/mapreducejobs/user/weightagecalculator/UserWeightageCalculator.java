package org.abhishaw.roadrate.mapreducejobs.user.weightagecalculator;

import java.io.IOException;
import java.util.NavigableMap;

import org.abhishaw.roadrate.dao.HbaseConfig;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class UserWeightageCalculator {
	public static class MyMapper extends TableMapper<Text, DoubleWritable> {

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			NavigableMap<byte[], byte[]> nm = value.getFamilyMap(Bytes.toBytes("UserRating"));
			for (byte[] key : nm.keySet()) {
				context.write(new Text(Bytes.toString(key)),
						new DoubleWritable(Math
								.abs(Bytes.toDouble(value.getValue(Bytes.toBytes("Summary"), Bytes.toBytes("Average")))
										- Double.parseDouble(Bytes.toString(nm.get(key))))));
			}
		}
	}

	public static class MyTableReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {

		@SuppressWarnings("deprecation")
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			Double i = 0.0;
			int count = 0;
			for (DoubleWritable val : values) {
				i += val.get() * 10;
				count++;
			}
			Double ans = 10.0;
			if (count > 0) {
				i /= count;
				ans *= (100 - i) / 100;
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Weightage"), Bytes.toBytes(ans));

			context.write(null, put);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(HbaseConfig.getHbaseConfiguration(), "UserWeightageCalculate");
		job.setJarByClass(UserWeightageCalculator.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob("road", scan, MyMapper.class, Text.class, DoubleWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("user", MyTableReducer.class, job);
		job.setNumReduceTasks(1);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
