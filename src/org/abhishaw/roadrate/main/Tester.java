package org.abhishaw.roadrate.main;

import java.io.IOException;
import java.net.Socket;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

public class Tester {

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		

		Socket socket = new Socket("localhost", 5555);
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

		JsonWriter jsonWriter = Json.createWriter(socket.getOutputStream());
		jsonWriter.writeObject(getUser);
		JsonReader jsonReader = Json.createReader(socket.getInputStream());
		jsonObject = jsonReader.readObject();
		System.out.println(jsonObject);
		socket.close();

	}
}