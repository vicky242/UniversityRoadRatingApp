package org.abhishaw.roadrate.service;

import java.io.IOException;

import javax.json.Json;
import javax.json.JsonObject;

import org.abhishaw.roadrate.dao.UserTableConstants;
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

		String address, phoneNumber, emailAddress, name;
		address = phoneNumber = emailAddress = name = "NULL";

		if (null != result.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.PERSONALINFORMATION),
				Bytes.toBytes(UserTableConstants.ColumnFamily.PersonalInformation.ADDRESS)))

			address = Bytes.toString(result.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.PERSONALINFORMATION),
					Bytes.toBytes(UserTableConstants.ColumnFamily.PersonalInformation.ADDRESS)));

		if (null != result.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.PERSONALINFORMATION),
				Bytes.toBytes(UserTableConstants.ColumnFamily.PersonalInformation.PHONENUMBER)))

			phoneNumber = Bytes
					.toString(result.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.PERSONALINFORMATION),
							Bytes.toBytes(UserTableConstants.ColumnFamily.PersonalInformation.PHONENUMBER)));

		if (null != result.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.PERSONALINFORMATION),
				Bytes.toBytes(UserTableConstants.ColumnFamily.PersonalInformation.EMAILID)))

			emailAddress = Bytes
					.toString(result.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.PERSONALINFORMATION),
							Bytes.toBytes(UserTableConstants.ColumnFamily.PersonalInformation.EMAILID)));
		if (null != result.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.PERSONALINFORMATION),
				Bytes.toBytes(UserTableConstants.ColumnFamily.PersonalInformation.NAME)))

			name = Bytes.toString(result.getValue(Bytes.toBytes(UserTableConstants.ColumnFamily.PERSONALINFORMATION),
					Bytes.toBytes(UserTableConstants.ColumnFamily.PersonalInformation.NAME)));

		JsonObject reply = Json.createObjectBuilder().add("RequestReply", "Successful").add("ReplyDetail", Json.createObjectBuilder().add("UserId", userId).add("PhoneNumber", phoneNumber)
				.add("EmailId", emailAddress).add("Name", name).add("Address", address)).build();
		return reply;
	}
}
