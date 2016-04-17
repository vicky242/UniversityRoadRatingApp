package org.abhishaw.roadrate.main;

import java.io.IOException;
import java.net.Socket;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

public class Tester {

	public static void main(String[] args) throws IOException, ClassNotFoundException {/*
		JsonObject jsonObject = Json.createObjectBuilder().add("UserId", "abhishaw").build();
		System.out.println(new UserRatedRoadService().processRequest(jsonObject).toString());*/
		
		Socket socket = new Socket("localhost", 5001);
		JsonObject jsonObject = Json.createObjectBuilder().add("RequestType", "GetuserDetails").add("RequestDetails", Json.createObjectBuilder().add("UserId", "abhishaw")).build();
		JsonWriter jsonWriter = Json.createWriter(socket.getOutputStream());
		jsonWriter.writeObject(jsonObject);
		JsonReader jsonReader = Json.createReader(socket.getInputStream());
		jsonObject = jsonReader.readObject();
		System.out.println(jsonObject);
		socket.close();
	}
}
