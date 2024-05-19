/**
 * Coordinator.java
 * Implements the coordinator node that receives requests from clients
 * and forwards them to the appropriate nodes in the system.
 */
package main.java.io.kvstore.coordinator;

import main.java.io.kvstore.Config;
import main.java.io.kvstore.store.StoreClient;
import main.java.io.kvstore.store.StoreMessage;
import main.java.io.kvstore.utils.Message;
import main.java.io.kvstore.utils.Utils;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class Coordinator {
	private final DatagramSocket _socket;

	/**
	 * The monitor that keeps track of the nodes in the system.
	 */
	private final Monitor _monitor;

	/**
	 * The client that sends requests to the store nodes.
	 */
	private final StoreClient _client;
	
	public Coordinator(int port, Monitor monitor, StoreClient client) throws SocketException
	{
		this._socket = new DatagramSocket(port);
		this._socket.setReuseAddress(true);
		this._monitor = monitor;
		this._client = client;
	}

	/**
	 * Serves requests from clients.
	 * @param executor
	 */
	public void serve(ExecutorService executor)
	{
		while (true)
		{
			byte[] request = new byte[Message.REQ_MAX_BYTES];
			DatagramPacket packet = new DatagramPacket(request, request.length);
			try {
				this._socket.receive(packet);
				executor.execute(() -> {
					byte[] id = Message.id(request);
					Message.Command command = Message.command(request);
					switch (command)
					{
						case PUT:
							this.put(packet);
							break;
						case GET:
							this.get(packet);
							break;
						case REMOVE:
							this.remove(packet);
							break;
						case SHUTDOWN:
							this.shutdown(packet);
							break;
						default:
							try {
								packet.setData(Message.createResponse(id, Message.Code.UNRECOGNIZED_COMMAND));
								this._socket.send(packet);
							} catch (Exception e) {
								e.printStackTrace();
							}
							break;
					}
				});
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Shuts down the coordinator.
	 * @param packet
	 */
	private void shutdown(DatagramPacket packet)
	{
		byte[] request = packet.getData();
		byte[] id = Message.id(request);
		try {
			packet.setData(Message.createResponse(id, Message.Code.SUCCESSFUL));
			this._socket.send(packet);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(1);
	}

	private void put(DatagramPacket packet)
	{
		byte[] request = packet.getData();
		byte[] id = Message.id(request);
		byte[] key = Message.key(request);
		byte[] val = Message.requestValue(request);
		put(packet, id, key, val);
	}

	private void remove(DatagramPacket packet)
	{
		byte[] request = packet.getData();
		byte[] id = Message.id(request);
		byte[] key = Message.key(request);
		put(packet, id, key, new byte[0]);
	}

	/**
	 * Sends put request to the store nodes.
	 * @param packet
	 * @param id
	 * @param key
	 * @param val
	 */
	private void put(DatagramPacket packet,  byte[] id, byte[] key, byte[] val)
	{
		byte[] storeRequest = StoreMessage.createPutRequest(id, key, val, 0);
		HashMap<Node, byte[]> responses = new HashMap<Node, byte[]>();
		Set<Node> nodes = this._monitor.getSuccessors(key, Config.REPLICATION_FACTOR);
		for (Node node : nodes) {
			Consumer<byte[]> onSuccess = res -> {
				/**
				 * ToDo: Implement the logic to handle the success responses from the store node.
				 * 1. Set the node available.
				 * 2. Lock the responses map.
				 * 3. If the votes are greater than or equal to the write quorum, send the response to the client.
				 *   3.1 Set the response code based on the store response.
				 *   3.2 Create the response message.
				 *   3.3 Send the response to the client.
				 */
				// code here

				synchronized (responses) { // lock the responses map
					// code here

					responses.put(node, res);
				}
			};
			
			Consumer<byte[]> onFailure = req -> {
				node.setAvailable(false);
			};
			this._client.send(node.getAddress(), node.getStorePort(), storeRequest, onSuccess, onFailure);
		}
	}

	/**
	 * Sends get request to the store nodes.
	 * @param packet
	 */
	private void get(DatagramPacket packet)
	{
		byte[] request = packet.getData();
		byte[] id = Message.id(request);
		byte[] key = Message.key(request);
		byte[] storeRequest = StoreMessage.createGetRequest(id, key);
		HashMap<Node, byte[]> responses = new HashMap<Node, byte[]>();
		Set<Node> nodes = _monitor.getSuccessors(key, Config.REPLICATION_FACTOR);
		for (Node node : nodes) {
			Consumer<byte[]> onSuccess = res -> {
				/**
				 * ToDo: Implement the logic to handle the success responses from the store node.
				 * 1. Set the node available.
				 * 2. Lock the responses map.
				 * 3. If the votes are greater than or equal to the read quorum, send the response to the client.
				 * 	 3.1 Select the response with the highest version.
				 * 	 3.3 Judge the response value whether it is empty or not.
				 *   3.4 Create the response message.
				 *   3.5 Send the response to the client.
				 *   3.6 If exists old value, send repair request to nodes. (optional)
				 */
				// code here
				synchronized (responses) { // lock the responses map
					// code here
					
					responses.put(node, res);
				}
			};
			
			Consumer<byte[]> onFailure = req -> {
				node.setAvailable(false);
			};
			
			_client.send(node.getAddress(), node.getStorePort(), storeRequest, onSuccess, onFailure);
		}
	}
}
