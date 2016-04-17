package org.abhishaw.roadrate.service;

import java.io.IOException;

import javax.json.JsonObject;

public interface ServiceInterface {
	public JsonObject processRequest(JsonObject jsonObject) throws IOException;
}
