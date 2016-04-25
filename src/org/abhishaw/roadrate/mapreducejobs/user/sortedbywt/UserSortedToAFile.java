package org.abhishaw.roadrate.mapreducejobs.user.sortedbywt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserSortedToAFile {

	public static class MyMapper extends TableMapper<DoubleWritable, Text> {

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			Text text = new Text();
			text.set(Bytes.toString(row.get()));
			context.write(new DoubleWritable(
					-Bytes.toDouble(value.getValue(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Weightage")))),
					text);
		}
	}

	public static class MyReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, new DoubleWritable(-key.get()));
			}

		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		Job job = new Job(config);
		job.setJarByClass(UserSortedToAFile.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob("user", scan, MyMapper.class, DoubleWritable.class, Text.class, job);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/myUserSummary"));
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}