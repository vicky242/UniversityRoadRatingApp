package org.abhishaw.roadrate.dao.writer;

import java.io.IOException;

import org.abhishaw.roadrate.dao.HbaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class RoadWriter {
	@SuppressWarnings("deprecation")
	public static void insert(Put put) throws IOException {
		Configuration configuration = HbaseConfig.getHbaseConfiguration();
		HTable hTable = new HTable(configuration, "road");

		hTable.put(put);
		hTable.close();
	}

}
