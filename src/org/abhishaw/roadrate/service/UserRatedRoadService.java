package org.abhishaw.roadrate.service;

import java.io.IOException;
import java.util.NavigableMap;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;

import org.abhishaw.roadrate.dao.reader.UserReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class UserRatedRoadService implements ServiceInterface{

	@Override
	public JsonObject processRequest(JsonObject jsonObject) throws IOException {
		String userId = jsonObject.getString("UserId");

		Get get = new Get(Bytes.toBytes(userId));
		get.addFamily(Bytes.toBytes("RoadRating"));
		Result result = UserReader.getUser(get);
		NavigableMap<byte[], byte[]> columnFamily = result.getFamilyMap(Bytes.toBytes("RoadRating"));
		JsonArrayBuilder roadListBuilder = Json.createArrayBuilder();
		for (byte[] road : columnFamily.keySet()) {
			roadListBuilder
					.add(Json.createObjectBuilder().add(Bytes.toString(road), Bytes.toString(columnFamily.get(road))));
		}
		JsonObject reply = Json.createObjectBuilder().add("RequestReply", "Successfull")
				.add("ListOfRoads", roadListBuilder).build();
		return reply;

	}
}
