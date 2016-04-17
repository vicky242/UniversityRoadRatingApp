package org.abhishaw.roadrate.service;

import java.io.IOException;

import javax.json.Json;
import javax.json.JsonObject;

import org.abhishaw.roadrate.dao.writer.RoadWriter;
import org.abhishaw.roadrate.dao.writer.UserWriter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class RateRoadService implements ServiceInterface {
	
	@Override
	public JsonObject processRequest(JsonObject jsonObject) throws IOException{
		String userId = jsonObject.getString("UserId");
		String roadId = jsonObject.getString("RoadId");
		String rating = jsonObject.getString("Rating");
		
		Put putIntoUser = new Put(Bytes.toBytes(userId));
		putIntoUser.addColumn(Bytes.toBytes("RoadRating"), Bytes.toBytes(roadId), Bytes.toBytes(rating));
		UserWriter.insert(putIntoUser);
		Put putIntoRoad = new Put(Bytes.toBytes(roadId));
		putIntoRoad.addColumn(Bytes.toBytes("UserRating"), Bytes.toBytes(userId), Bytes.toBytes(rating));
		RoadWriter.insert(putIntoRoad);
		
		JsonObject reply = Json.createObjectBuilder().add("RequestReply", "Successful").build();
		return reply;
	}
}
