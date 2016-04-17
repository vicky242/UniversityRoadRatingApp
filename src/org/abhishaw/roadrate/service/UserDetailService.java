package org.abhishaw.roadrate.service;

import java.io.IOException;

import javax.json.Json;
import javax.json.JsonObject;

import org.abhishaw.roadrate.dao.reader.UserReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class UserDetailService implements ServiceInterface {
	@Override
	public JsonObject processRequest(JsonObject jsonObject) throws IOException {
		String userId = jsonObject.getString("UserId");

		Get get = new Get(Bytes.toBytes(userId));
		get.addFamily(Bytes.toBytes("PersonalInformation"));
		Result result = UserReader.getUser(get);
		String address = Bytes
				.toString(result.getValue(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Address")));
		String phoneNumber = Bytes
				.toString(result.getValue(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("PhoneNumber")));
		String emailAddress = Bytes
				.toString(result.getValue(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("EmailId")));
		String name = Bytes.toString(result.getValue(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Name")));

		JsonObject reply = Json.createObjectBuilder().add("UserId", userId).add("PhoneNumber", phoneNumber)
				.add("EmailId", emailAddress).add("Name", name).add("Address", address).build();
		return reply;
	}
}
