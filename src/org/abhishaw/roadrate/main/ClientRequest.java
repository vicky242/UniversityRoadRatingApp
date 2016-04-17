package org.abhishaw.roadrate.main;

import java.io.IOException;
import java.net.Socket;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

import org.abhishaw.roadrate.service.ServiceFactory;
import org.abhishaw.roadrate.service.ServiceInterface;

public class ClientRequest implements Runnable {

	private Socket clientSocket;

	public ClientRequest(Socket clSocket) {
		clientSocket = clSocket;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		JsonReader jsRdr;
		try {
			jsRdr = Json.createReader(clientSocket.getInputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		JsonObject jsonObject = jsRdr.readObject(), reply = null;
		String requestType = jsonObject.getString("RequestType");
		ServiceInterface service = ServiceFactory.getService(requestType);

		try {
			reply = service.processRequest(jsonObject.getJsonObject("RequestDetails"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			JsonWriter jsonWriter = Json.createWriter(clientSocket.getOutputStream());
			jsonWriter.writeObject(reply);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			clientSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
