package org.abhishaw.roadrate.mapreducejobs.user.weightageinti;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class UserWeightInitializerMapper {
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(row.get());
			put.addColumn(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Weightage"), Bytes.toBytes(new Double(10)));
			context.write(null, put);
		}

	}
}