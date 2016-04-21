package org.abhishaw.roadrate.main;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ServerStarter {
	private static final Logger logger = LogManager.getLogger(ServerStarter.class.getName());
	final private int portNumber = 5001;
	private ServerSocket serverSocket;

	private ServerStarter() throws IOException {
		serverSocket = new ServerSocket(portNumber);
	}

	private ClientRequest listen() throws IOException {
		try {
			System.out.println("listening for request.....");
			Socket clientSocket = serverSocket.accept();
			ClientRequest client = new ClientRequest(clientSocket);
			return client;

		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public static void main(String[] args) throws IOException {
		ServerStarter localServer = new ServerStarter();
		try {
			while (true) {
				ClientRequest temp;
				try {
					temp = localServer.listen();
					new Thread(temp).start();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} finally {
			localServer.close();
		}
	}

	private void close(){
		try {
			serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
