package org.abhishaw.roadrate.dao.reader;

import java.io.IOException;

import org.abhishaw.roadrate.dao.HbaseConfig;
import org.abhishaw.roadrate.dao.UserTableConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class UserReader {
	public static Result getUser(Get get) throws IOException {
		Configuration configuration = HbaseConfig.getHbaseConfiguration();
		@SuppressWarnings("deprecation")
		HTable table = new HTable(configuration, UserTableConstants.TABLENAME);
		// Reading the data
		Result result = table.get(get);
		table.close();
		return result;
	}

}
