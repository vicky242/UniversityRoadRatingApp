package org.abhishaw.roadrate.service;

import java.io.IOException;

import javax.json.Json;
import javax.json.JsonObject;

import org.abhishaw.roadrate.dao.reader.UserReader;
import org.abhishaw.roadrate.dao.writer.UserWriter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class NewAccountService implements ServiceInterface{
	
	@Override
	@SuppressWarnings("deprecation")
	public JsonObject processRequest(JsonObject jsonObject) throws IOException{
		String userId = jsonObject.getString("UserId");
		String password = jsonObject.getString("Password");
		Get get = new Get(Bytes.toBytes(userId));
		Result result = UserReader.getUser(get);
		JsonObject reply = null;
		if (result.isEmpty()){
			Put put = new Put(Bytes.toBytes(userId));
			put.add(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Password"), Bytes.toBytes(password));
			UserWriter.insert(put);
			reply = Json.createObjectBuilder().add("RequestReply", "AccountCreated").build();
		}
		else{
			reply = Json.createObjectBuilder().add("RequestReply", "UserIdAlreadyExist").build();
		}
		return reply;
	}
}
