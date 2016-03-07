import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RoadDatabaseDAO {
	public RoadDatabaseDAO(Configuration config) {
		super();
		this.config = config;
	}

	// creating a configuration object
	private final Configuration config;

	@SuppressWarnings("deprecation")
	private void insertSingleRow(String rowid, Integer rating) throws IOException {
		// Instantiating HTable class
		HTable hTable = new HTable(config, "Roads");

		// Instantiating Put class
		// accepts a row name.
		Put p = new Put(Bytes.toBytes(rowid));

		p.add(Bytes.toBytes("User Input"), Bytes.toBytes("Sum"), Bytes.toBytes(rating.toString()));
		p.add(Bytes.toBytes("User Input"), Bytes.toBytes("Count"), Bytes.toBytes("1"));
		hTable.put(p);
		hTable.close();
	}

	public Result getSingleRow(String rowid) throws IOException {
		@SuppressWarnings("deprecation")
		HTable table = new HTable(config, "Roads");

		// Instantiating Get class
		Get g = new Get(Bytes.toBytes(rowid));

		// Reading the data
		Result result = table.get(g);
		table.close();
		return result;
	}
	public static Record toRecord(Result result){
		Record record = new Record();
		record.setRoadId(Bytes.toString(result.getRow()));
		record.setRating(Bytes.toString(result.getValue(Bytes.toBytes("User Input"), Bytes.toBytes("Sum"))));
		record.setUsers(Bytes.toString(result.getValue(Bytes.toBytes("User Input"), Bytes.toBytes("Count"))));
		return record;
	}
	@SuppressWarnings("deprecation")
	synchronized public void updateSingleRow(String rowid, Integer rating) throws IOException {

		Result result = getSingleRow(rowid);
		if (result.isEmpty()) {
			insertSingleRow(rowid, rating);
		} else {
			Integer sum = rating, count = Integer.valueOf(1);
			byte[] value = result.getValue(Bytes.toBytes("User Input"), Bytes.toBytes("Sum"));
			sum = sum + Integer.parseInt(Bytes.toString(value));
			value = result.getValue(Bytes.toBytes("User Input"), Bytes.toBytes("Count"));
			count = count + Integer.parseInt(Bytes.toString(value));

			HTable hTable = new HTable(config, "Roads");

			// Instantiating Put class
			// accepts a row name.
			Put p = new Put(Bytes.toBytes(rowid));

			p.add(Bytes.toBytes("User Input"), Bytes.toBytes("Sum"), Bytes.toBytes(sum.toString()));
			p.add(Bytes.toBytes("User Input"), Bytes.toBytes("Count"), Bytes.toBytes(count.toString()));
			hTable.put(p);
			hTable.close();
		}
	}

	public HTableDescriptor[] listTables() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		@SuppressWarnings("deprecation")
		HBaseAdmin admin = new HBaseAdmin(config);

		HTableDescriptor[] tableDescriptor = admin.listTables();
		admin.close();
		return tableDescriptor;
	}
	
	
}
