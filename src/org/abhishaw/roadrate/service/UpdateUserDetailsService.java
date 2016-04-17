package org.abhishaw.roadrate.service;

import java.io.IOException;

import javax.json.Json;
import javax.json.JsonObject;

import org.abhishaw.roadrate.dao.writer.UserWriter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class UpdateUserDetailsService implements ServiceInterface{

	@Override
	@SuppressWarnings("deprecation")
	public JsonObject processRequest(JsonObject jsonObject) throws IOException {

		String address = (jsonObject.containsKey("Address") ? jsonObject.getString("Address") : null);
		String phoneNumber = (jsonObject.containsKey("PhoneNumber") ? jsonObject.getString("PhoneNumber") : null);
		String emailAddress = (jsonObject.containsKey("EmailId") ? jsonObject.getString("EmailId") : null);
		String name = (jsonObject.containsKey("Name") ? jsonObject.getString("Name") : null);
		String userId = jsonObject.getString("UserId");
		Put put = new Put(Bytes.toBytes(userId));

		if (address != null)
			put.add(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Address"), Bytes.toBytes(address));

		if (name != null)
			put.add(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Name"), Bytes.toBytes(name));

		if (emailAddress != null)
			put.add(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("EmailId"), Bytes.toBytes(emailAddress));

		if (phoneNumber != null)
			put.add(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("PhoneNumber"), Bytes.toBytes(phoneNumber));

		UserWriter.insert(put);

		return Json.createObjectBuilder().add("RequestReply", "SuccessFul").build();
	}
}
