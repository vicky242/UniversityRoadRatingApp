package org.abhishaw.roadrate.dao.writer;

import java.io.IOException;

import org.abhishaw.roadrate.dao.HbaseConfig;
import org.abhishaw.roadrate.dao.UserTableConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class UserWriter {
	@SuppressWarnings("deprecation")
	public static void insert(Put put) throws IOException {
		Configuration configuration = HbaseConfig.getHbaseConfiguration();
		// Instantiating HTable class
		HTable hTable = new HTable(configuration, UserTableConstants.TABLENAME);
		hTable.put(put);
		hTable.close();
	}

}
