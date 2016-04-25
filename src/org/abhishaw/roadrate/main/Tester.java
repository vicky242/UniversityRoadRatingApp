package org.abhishaw.roadrate.main;

import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

public class Tester {

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		JsonReader jsonReader = Json.createReader(new FileReader("/home/abhishaw/Downloads/MOCK_DATA.json"));
		JsonArray jsonArray = jsonReader.readArray();
		System.out.println(jsonArray);
		for (int i = 0; i < jsonArray.size(); i++) {
			JsonObject data = jsonArray.getJsonObject(i);
			JsonObject newAcc = Json.createObjectBuilder()
					.add("RequestType", "NewUserAccount").add("RequestDetails", Json.createObjectBuilder()
							.add("UserId", data.getString("UserId")).add("Password", data.getString("Password")))
					.build();
			System.out.println(i + " = " + newAcc);
			post(newAcc);
			JsonObject upUserDetial = Json.createObjectBuilder().add("RequestType", "UpdateUserInfo")
					.add("RequestDetails", Json.createObjectBuilder().add("UserId", data.getString("UserId"))
							.add("Address", data.getString("Address")).add("PhoneNumber", data.getString("PhoneNumber"))
							.add("EmailId", data.getString("EmailId")).add("Name", data.getString("Name")))
					.build();
			System.out.println(i + " = " + upUserDetial);
			post(upUserDetial);

		}
		JsonObject jsonObject = Json.createObjectBuilder().add("RequestType", "RatingRoad")
				.add("RequestDetails",
						Json.createObjectBuilder().add("UserId", "abhishaw").add("RoadId", "234").add("Rating", "8"))
				.build();
		System.out.println(jsonObject);
		JsonObject loginType = Json.createObjectBuilder().add("RequestType", "Login").add("RequestDetails",
				Json.createObjectBuilder().add("UserId", "abhishaw").add("Password", "*********")).build();
		System.out.println(loginType);
		JsonObject newAcc = Json.createObjectBuilder().add("RequestType", "NewUserAccount").add("RequestDetails",
				Json.createObjectBuilder().add("UserId", "abhishaw").add("Password", "*********")).build();
		System.out.println(newAcc);
		JsonObject upUserDetial = Json.createObjectBuilder().add("RequestType", "UpdateUserInfo")
				.add("RequestDetails",
						Json.createObjectBuilder().add("UserId", "abhishaw").add("Address", "jjhfkjjadsf")
								.add("PhoneNumber", "8732648").add("EmailId", "gajfhgs@kjsdfh.com")
								.add("Name", "ANblfsd Kkjaf"))
				.build();
		System.out.println(upUserDetial);

		JsonObject getUser = Json.createObjectBuilder().add("RequestType", "GetuserDetails")
				.add("RequestDetails", Json.createObjectBuilder().add("UserId", "abhishaw")).build();
		System.out.println(getUser);
		JsonObject list = Json.createObjectBuilder().add("RequestType", "GetUserRatedRoads")
				.add("RequestDetails", Json.createObjectBuilder().add("UserId", "abhishaw")).build();
		System.out.println(list);
		post(getUser);
	}

	private static void post(JsonObject newAcc) throws UnknownHostException, IOException {
		Socket socket = new Socket("localhost", 5001);
		JsonWriter jsonWriter = Json.createWriter(socket.getOutputStream());
		jsonWriter.writeObject(newAcc);
		JsonReader jsonReader2 = Json.createReader(socket.getInputStream());
		JsonObject jsonObject = jsonReader2.readObject();
		System.out.println(jsonObject);
		socket.close();

		
	}
}