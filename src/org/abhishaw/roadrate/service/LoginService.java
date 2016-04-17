package org.abhishaw.roadrate.service;

import java.io.IOException;

import javax.json.Json;
import javax.json.JsonObject;

import org.abhishaw.roadrate.dao.reader.UserReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class LoginService implements ServiceInterface{
	@Override
	public JsonObject processRequest(JsonObject jsonObject) throws IOException{
		String userId = jsonObject.getString("UserId");
		String password = jsonObject.getString("Password");
		
		Get get = new Get(Bytes.toBytes(userId));
		get.addColumn(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Password"));
		Result result = UserReader.getUser(get);
		String databasePass = Bytes.toString(result.getValue(Bytes.toBytes("PersonalInformation"), Bytes.toBytes("Password")));
		JsonObject reply = null;
		if (databasePass.equals(password)){
			reply= Json.createObjectBuilder().add("RequestReply", "AccessGranted").build();
		}
		else{
			reply= Json.createObjectBuilder().add("RequestReply", "IncorrectPassword").build();
		}
		return reply;
	}
}
