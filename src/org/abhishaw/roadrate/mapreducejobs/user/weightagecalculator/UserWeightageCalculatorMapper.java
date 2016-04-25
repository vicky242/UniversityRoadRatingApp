package org.abhishaw.roadrate.mapreducejobs.user.weightagecalculator;

import java.io.IOException;
import java.util.NavigableMap;

import org.abhishaw.roadrate.dao.reader.RoadReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class UserWeightageCalculatorMapper {
	public static class MyMapper extends TableMapper<Text, DoubleWritable> {

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			System.out.println("new Mapper for " + Bytes.toString(row.get()));
			NavigableMap<byte[], byte[]> nm = value.getFamilyMap(Bytes.toBytes("UserRating"));
			for (byte[] key : nm.keySet()) {
				context.write(new Text(Bytes.toString(key)),
						new DoubleWritable(Math
								.abs(Bytes.toDouble(value.getValue(Bytes.toBytes("Summary"), Bytes.toBytes("Average")))
										- Double.parseDouble(Bytes.toString(nm.get(key))))));
				System.out.println(Bytes.toString(key) + " road: " + Bytes.toString(row.get()) + " rating: "
						+ Double.parseDouble(Bytes.toString(nm.get(key))));
			}
		}
	}

	public static class MyTableReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {

		@SuppressWarnings("deprecation")
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Finally here!!");
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
			System.out.println(key.toString() + ": " + ans);
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Weightage"), Bytes.toBytes(ans));

			context.write(null, put);
		}
	}
}
