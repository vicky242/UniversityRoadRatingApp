import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Server {
	private List<ClientRequest> clients;
	final private int portNumber = 5001;
	private ServerSocket serverSocket;
	RoadDatabaseDAO roadDatabaseDao;
	Configuration config;
	private Server(){
		clients = new ArrayList<ClientRequest>();
		try {
			serverSocket = new ServerSocket(portNumber);
			config = HBaseConfiguration.create();
			roadDatabaseDao = new RoadDatabaseDAO(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private ClientRequest listen() throws IOException{
		try {
			System.out.println("listening for request.....");
			Socket clientSocket = serverSocket.accept();
			ClientRequest client = new ClientRequest(clientSocket, roadDatabaseDao);
			return client;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
	}
	
	public static void main(String[] args){
		Server localServer = new Server();
		try {
			while (true) {
				ClientRequest temp;
				try {
					temp = localServer.listen();
					temp.start();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} 
		} finally {
			// TODO: handle finally clause
			localServer.close();
		}
	}
	private void close(){
		try {
			serverSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
