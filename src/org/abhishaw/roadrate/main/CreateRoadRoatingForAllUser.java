package org.abhishaw.roadrate.main;

import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.abhishaw.roadrate.dao.HbaseConfig;
import org.abhishaw.roadrate.service.RateRoadService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateRoadRoatingForAllUser {
	static Random select = new Random();

	public static void main(String[] args) throws IOException {
		JsonReader jsonReader = Json.createReader(new FileReader("/home/abhishaw/Downloads/MOCK_DATA.json"));
		JsonArray jsonArray = jsonReader.readArray();
		long totaltime = System.currentTimeMillis();
		for (int i = 11; i < jsonArray.size(); i++) {
			long start = System.currentTimeMillis();
			DeleteUser(jsonArray.getJsonObject(i).getString("UserId"), i);
			long end = System.currentTimeMillis();
			System.out.println("Time taken : " + (end - start + 1) / 1000);
		}
		System.out.println(
				"Total time taken to Complete 199 entries : " + (System.currentTimeMillis() - totaltime) / 1000);
	}

	private static void DeleteUser(String string, int idx) throws IOException {
		Configuration configuration = HbaseConfig.getHbaseConfiguration();
		@SuppressWarnings("deprecation")
		HTable table = new HTable(configuration, "road");
		for (int i = 1;i<=1000;i++){
			Delete delete = new Delete(Bytes.toBytes(String.valueOf(i)));
			delete.addColumn(Bytes.toBytes("UserRating"), Bytes.toBytes(string));
			table.delete(delete);
		}
		table.close();
		
	}

	private static void createRoadRateRequestDetail(String string, int idx) throws UnknownHostException, IOException {
		int count = 0;
		for (int i = 1; i <= 1000; i++) {
			if (select.nextBoolean() &&select.nextBoolean() && select.nextBoolean() && select.nextBoolean()) {
				Integer rating = Math.abs(select.nextInt()) % 11;
				JsonObject jsonObject = Json.createObjectBuilder().add("RequestType", "RatingRoad")
						.add("RequestDetails",
								Json.createObjectBuilder().add("UserId", string)
										.add("RoadId", Integer.valueOf(i).toString()).add("Rating", rating.toString()))
						.build();
				//System.out.println(String.valueOf(idx) + " = " + jsonObject);
				//System.out.print(String.valueOf(idx) + " = ");
				post(jsonObject);
				count++;
			}
		}
		System.out.println(idx + " = " + count);
	}

	private static void post(JsonObject newAcc) throws UnknownHostException, IOException {
		RateRoadService rateRoadService = new RateRoadService();
		rateRoadService.processRequest(newAcc.getJsonObject("RequestDetails"));
		/*
		 * Socket socket = new Socket("localhost", 5001); JsonWriter jsonWriter
		 * = Json.createWriter(socket.getOutputStream());
		 * jsonWriter.writeObject(newAcc); JsonReader jsonReader2 =
		 * Json.createReader(socket.getInputStream()); JsonObject jsonObject =
		 * jsonReader2.readObject(); System.out.println(jsonObject);
		 * socket.close();
		 */

	}
}
