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
			int count = 0;
			Double sum = 0.0;
			NavigableMap<byte[], byte[]> nm = value.getFamilyMap(Bytes.toBytes("UserRating"));
			for (byte[] key : nm.keySet()) {
				count++;
				sum += Double.parseDouble(Bytes.toString(nm.get(key)));
			}
			if (count > 0)
				sum /= count;
			Put put = new Put(row.get());
			put.add(Bytes.toBytes("Summary"), Bytes.toBytes("Average"), Bytes.toBytes(sum));
			context.write(row, put);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(HbaseConfig.getHbaseConfiguration(), "RoadAverageCalculator");
		job.setJarByClass(RoadAverageCalculator.class); // class that
																// contains
																// mapper and
		// reducer

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob("road", // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				null, // mapper output key
				null, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob("road", // output table
				null, // reducer class
				job);
		job.setNumReduceTasks(0); // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}
