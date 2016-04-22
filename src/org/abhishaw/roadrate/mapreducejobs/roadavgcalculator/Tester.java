package org.abhishaw.roadrate.mapreducejobs.roadavgcalculator;

import java.io.IOException;

import org.abhishaw.roadrate.dao.reader.RoadReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Tester {
	public static void main(String[] args) throws IOException {
		Get get = new Get(Bytes.toBytes("234"));
		get.addColumn(Bytes.toBytes("Summary"), Bytes.toBytes("Average"));
		Result result = RoadReader.getRoad(get);
		double avg = Bytes.toDouble(result.getValue(Bytes.toBytes("Summary"), Bytes.toBytes("Average")));
		System.out.println(avg);
	}
}
