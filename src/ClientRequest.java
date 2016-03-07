import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class ClientRequest extends Thread{
	private Socket clientSocket;
	private RoadDatabaseDAO roadDatabseDAO ;
	private BufferedReader clientBufferReader;
	public ClientRequest(Socket clientSocket, RoadDatabaseDAO roadDatabaseDao) {
		// TODO Auto-generated constructor stub
		this.clientSocket =clientSocket;
		this.roadDatabseDAO = roadDatabaseDao;
		
		try {
			clientBufferReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		Record record = null;
		try {
			try {
				record = recieveRecord();
				System.out.println(record);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}finally{
				clientSocket.close();
			}
			clientSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		try {
			roadDatabseDAO.updateSingleRow(record.getRoadId(), record.getRating());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Record recieveRecord() throws IOException {
		// TODO Auto-generated method stub
		String str = clientBufferReader.readLine();
		String [] tokens = str.split(",");
		Record record = new Record(tokens[0], tokens[1]);
		return record;
	}

}
