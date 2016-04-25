package org.abhishaw.roadrate.mapreducejobs.road.wtedavgcalc;

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

public class RoadWeightedAverageCalculator {
	public static class MyMapper extends TableMapper<Text, DoubleWritable> {

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			try {
				NavigableMap<byte[], byte[]> nm = value.getFamilyMap(Bytes.toBytes("RoadRating"));
				for (byte[] key : nm.keySet()) {
					context.write(new Text(Bytes.toString(key)),
							new DoubleWritable(Bytes
									.toDouble(value.getValue(Bytes.toBytes("PersonalInformation"),
											Bytes.toBytes("Weightage")))
									* Double.parseDouble(Bytes.toString(nm.get(key))) / 10));
				}
			} catch (Exception e) {
				e.printStackTrace();
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
				i += val.get();
				count++;
			}
			Double ans = 0.0;
			if (count > 0) {
				i /= count;
				ans = i;
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("Summary"), Bytes.toBytes("WeightedAverage"), Bytes.toBytes(ans));
			context.write(null, put);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(HbaseConfig.getHbaseConfiguration(), "RoadWeightedAverageCalculator");
		job.setJarByClass(RoadWeightedAverageCalculator.class);
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob("user", scan, MyMapper.class, Text.class, DoubleWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("road", MyTableReducer.class, job);
		job.setNumReduceTasks(1);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
