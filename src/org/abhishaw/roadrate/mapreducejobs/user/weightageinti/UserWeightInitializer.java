package org.abhishaw.roadrate.mapreducejobs.user.weightageinti;

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

public class UserWeightInitializer {
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(row.get());
			put.addColumn(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Weightage"),
					Bytes.toBytes(new Double(10)));
			context.write(null, put);
		}

	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(HbaseConfig.getHbaseConfiguration(), "UserWeightageInitialization");
		job.setJarByClass(UserWeightInitializer.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob("user", scan, MyMapper.class, null, null, job);
		TableMapReduceUtil.initTableReducerJob("user", null, job);
		job.setNumReduceTasks(0);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
