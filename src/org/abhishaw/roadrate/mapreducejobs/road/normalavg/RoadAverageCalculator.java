package org.abhishaw.roadrate.mapreducejobs.road.normalavg;

import java.io.IOException;
import java.util.NavigableMap;

import org.abhishaw.roadrate.dao.HbaseConfig;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class RoadAverageCalculator {
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

		@SuppressWarnings("deprecation")
		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(row.get());
			int count = 0;
			Double sum = 0.0;
			Integer sum2 = 0;
			NavigableMap<byte[], byte[]> nm = value.getFamilyMap(Bytes.toBytes("UserRating"));
			for (byte[] key : nm.keySet()) {
				count++;
				sum += Double.parseDouble(Bytes.toString(nm.get(key)));
				sum2 += Integer.parseInt(Bytes.toString(nm.get(key))); 
			}
			put.add(Bytes.toBytes("Summary"), Bytes.toBytes("Sum"), Bytes.toBytes(Integer.toString(sum2)));
			put.add(Bytes.toBytes("Summary"), Bytes.toBytes("Count"), Bytes.toBytes(Integer.toString(count)));
			if (count > 0)
				sum /= count;
			put.add(Bytes.toBytes("Summary"), Bytes.toBytes("Average"), Bytes.toBytes(sum));
			context.write(row, put);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(HbaseConfig.getHbaseConfiguration(), "RoadAverageCalculator");
		job.setJarByClass(RoadAverageCalculator.class);

		Scan scan = new Scan();
		scan.setCaching(500);

		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob("road", scan, MyMapper.class, null, null, job);
		TableMapReduceUtil.initTableReducerJob("road", null, job);
		job.setNumReduceTasks(0);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}
