package org.abhishaw.roadrate.service;

import java.io.IOException;

import javax.json.Json;
import javax.json.JsonObject;

import org.abhishaw.roadrate.dao.UserTableConstants;
import org.abhishaw.roadrate.dao.reader.RoadReader;
import org.abhishaw.roadrate.dao.reader.UserReader;
import org.abhishaw.roadrate.dao.writer.RoadWriter;
import org.abhishaw.roadrate.dao.writer.UserWriter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RateRoadService implements ServiceInterface {

	@Override
	public JsonObject processRequest(JsonObject jsonObject) throws IOException {
		String userId = jsonObject.getString("UserId");
		String roadId = jsonObject.getString("RoadId");
		String rating = jsonObject.getString("Rating");

		Get getFromUser = new Get(Bytes.toBytes(userId));
		getFromUser.addColumn(Bytes.toBytes(UserTableConstants.ColumnFamily.RATING), Bytes.toBytes(roadId));
		Result previousRating = UserReader.getUser(getFromUser);

		Put putIntoUser = new Put(Bytes.toBytes(userId));
		putIntoUser.addColumn(Bytes.toBytes(UserTableConstants.ColumnFamily.RATING), Bytes.toBytes(roadId),
				Bytes.toBytes(rating));
		UserWriter.insert(putIntoUser);

		Get getFromRoad = new Get(Bytes.toBytes(roadId));
		getFromRoad.addFamily(Bytes.toBytes("Summary"));

		Result summaryRoad = RoadReader.getRoad(getFromRoad);
		Integer sum = 0, count = 0;

		if (previousRating.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.RATING),
				Bytes.toBytes(roadId)) != null) {
			sum = -Integer.parseInt(Bytes.toString(previousRating
					.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.RATING), Bytes.toBytes(roadId))));
			count = -1;
		}
		if (null != summaryRoad.getValue(Bytes.toBytes("Summary"), Bytes.toBytes("Sum"))) {
			sum += Integer
					.parseInt(Bytes.toString(summaryRoad.getValue(Bytes.toBytes("Summary"), Bytes.toBytes("Sum"))));
			count += Integer
					.parseInt(Bytes.toString(summaryRoad.getValue(Bytes.toBytes("Summary"), Bytes.toBytes("Count"))));
		}
		sum += Integer.parseInt(rating);
		count += 1;
		Put putIntoRoad = new Put(Bytes.toBytes(roadId));
		putIntoRoad.addColumn(Bytes.toBytes("UserRating"), Bytes.toBytes(userId), Bytes.toBytes(rating));
		putIntoRoad.addColumn(Bytes.toBytes("Summary"), Bytes.toBytes("Count"), Bytes.toBytes(count.toString()));
		putIntoRoad.addColumn(Bytes.toBytes("Summary"), Bytes.toBytes("Sum"), Bytes.toBytes(sum.toString()));

		RoadWriter.insert(putIntoRoad);
		JsonObject reply = Json.createObjectBuilder().add("RequestReply", "Successful").build();
		return reply;
	}
}
