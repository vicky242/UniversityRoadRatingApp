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

public class RoadsSorter {

	public static class RoadsMapper extends TableMapper<DoubleWritable, Text> {

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {

			Double avg;

			Text text = new Text();
			text.set(Bytes.toString(value.getRow()));

			Integer sum = Integer
					.parseInt(Bytes.toString(value.getValue(Bytes.toBytes("User Input"), Bytes.toBytes("Sum"))));
			if (sum != 0) {
				avg = sum.doubleValue();
				Integer count = Integer
						.parseInt(Bytes.toString(value.getValue(Bytes.toBytes("User Input"), Bytes.toBytes("Count"))));
				avg /= count.doubleValue();
				context.write(new DoubleWritable(avg), text);
			}
		}
	}

	public static class MyReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}

		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		Job job = new Job(config);
		job.setJarByClass(RoadsSorter.class); // class that contains mapper and
												// reducer

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attributes

		TableMapReduceUtil.initTableMapperJob("Roads", // input table
				scan, // Scan instance to control CF and attribute selection
				RoadsMapper.class, // mapper class
				DoubleWritable.class, // mapper output key
				Text.class, // mapper output value
				job);
		job.setReducerClass(MyReducer.class); // reducer class
		job.setNumReduceTasks(1); // at least one, adjust as required
		FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/mySummaryFile")); // adjust
																				// directories
																				// as
																				// required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}
