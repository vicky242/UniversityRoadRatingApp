import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

public class Starter {

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration config = HBaseConfiguration.create();
		RoadDatabaseDAO db = new RoadDatabaseDAO(config);
		HTableDescriptor[] tables = db.listTables();
		for (HTableDescriptor table: tables){
			System.out.println(table.getNameAsString());
		}
		db.updateSingleRow("2", 10);
		db.updateSingleRow("3", 3);
		db.updateSingleRow("2", 1);
		Record record = RoadDatabaseDAO.toRecord(db.getSingleRow("2"));
		System.out.println(record);
	}

}
