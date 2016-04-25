package org.abhishaw.roadrate.mapreducejobs.road.wtedavgInit;

import java.io.IOException;

import org.abhishaw.roadrate.dao.HbaseConfig;
import org.abhishaw.roadrate.mapreducejobs.road.wtedavgInit.RoadWeightAvgInitializerMapper.MyMapper;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

public class RoadWeightAvgInitializer {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(HbaseConfig.getHbaseConfiguration(), "RoadWeightAvgInitializer");
		job.setJarByClass(RoadWeightAvgInitializerMapper.class); // class that
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

		System.out.println("Abhishekh");
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
