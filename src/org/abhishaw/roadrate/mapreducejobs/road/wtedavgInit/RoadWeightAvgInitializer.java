package org.abhishaw.roadrate.mapreducejobs.road.wtedavgInit;

import java.io.IOException;

import org.abhishaw.roadrate.dao.HbaseConfig;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class RoadWeightAvgInitializer {
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(row.get());
			put.addColumn(Bytes.toBytes("Summary"), Bytes.toBytes("WeightedAverage"), Bytes.toBytes(new Double(0)));
			context.write(null, put);
		}

	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(HbaseConfig.getHbaseConfiguration(), "RoadWeightAvgInitializer");
		job.setJarByClass(RoadWeightAvgInitializer.class);

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
