/**
 * Store.java
 * Store class is responsible for storing key-value pairs and serving requests from clients.
 */
package main.java.io.kvstore.store;

import main.java.io.kvstore.utils.Utils;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class Store
{
	/**
	 * Value class is used to store the value and version of a key-value pair.
	 */
	class Value
	{
		final byte[] value;
		final int version;
		Value(byte[] val, int ver)
		{
			this.value = val;
			this.version = ver;
		}
	}

	/**
	 * DatagramSocket object to receive and send packets.
	 */
	private final DatagramSocket _socket;

	/**
	 * ConcurrentHashMap to store key-value pairs.
	 */
	private final ConcurrentHashMap<String, Value> _store;

	/**
	 * Constructor to initialize the Store object and the DatagramSocket object.
	 * @param port
	 * @throws SocketException
	 */
	public Store(int port) throws SocketException
	{
		this._store = new ConcurrentHashMap<String, Value>();
		this._socket = new DatagramSocket(port);
		this._socket.setReuseAddress(true);
	}

	/**
	 * Method to serve requests from clients.
	 * @param executor
	 */
	public void serve(ExecutorService executor)
	{
		while (true)
		{
			byte[] request = new byte[StoreMessage.MAX_REQ_BYTES];
			DatagramPacket packet = new DatagramPacket(request, request.length); // initialize packet
			try {
				this._socket.receive(packet); // receive packet
				executor.execute(() -> { // execute the request in a separate thread
					byte[] id = StoreMessage.id(request);
					byte[] key = StoreMessage.key(request);
					byte[] response;
					switch (StoreMessage.storeRequestType(request))
					{
						case PUT:
							response = put(id, key, StoreMessage.requestValue(request), StoreMessage.requestValueVersion(request));
							break;
						case GET:
							response = get(id, key);
							break;
						case HEARTBEAT_REQ:
							response = StoreMessage.createResponse(id, StoreMessage.StoreResponseType.HEARTBEAT_ACK);
							break;
						default:
							response = StoreMessage.createResponse(id, StoreMessage.StoreResponseType.UNRECOGNIZED_COMMAND);
							break;
					}
					packet.setData(response);
					try {
						this._socket.send(packet);
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * method to get the value of a key from the store.
	 * @param id: request id
	 * @param key: key
	 * @return response (id, response type, version, value)
	 */
	private byte[] get(byte[] id, byte[] key)
	{
		String keyString = Utils.hexString(key);

		/**
		 * If the key does not exist in the store, create a new key-value pair with an empty value.
		 * This is to avoid null pointer exception when trying to access the value of a non-existent key
		 * and to return uniform responses for all keys.
		 * You can also return a response with response type NON_EXISTENT_KEY. (Optional)
		 */
		if (!this._store.containsKey(keyString))
		{
			this._store.put(keyString, new Value(new byte[0], 0));
		}

		Value currVal = this._store.get(keyString);

		StoreMessage.StoreResponseType response = currVal.value.length > 0 ? StoreMessage.StoreResponseType.SUCCESSFUL : StoreMessage.StoreResponseType.NON_EXISTENT_KEY;
		
		return StoreMessage.createResponse(id, response, currVal.version, currVal.value);
	}

	/**
	 * method to put a key-value pair in the store.
	 * @param id: request id
	 * @param key: key
	 * @param val: value
	 * @param ver: version
	 * @return
	 */
	private byte[] put(byte[] id, byte[] key, byte[] val, int ver)
	{
		String keyString = Utils.hexString(key);

		/**
		 * Update the value of the key if the version is greater than 0.
		 * If value's length == 0, the key does not exist in the store.
		 */
		if (ver > 0)
		{
			this._store.put(keyString, new Value(val, ver));
			return StoreMessage.createResponse(id, StoreMessage.StoreResponseType.SUCCESSFUL);
		}

		/**
		 * If the key does not exist in the store, create a new key-value pair with an empty value.
		 */
		if (!this._store.containsKey(keyString))
		{
			this._store.put(keyString, new Value(new byte[0], 0));
		}
		Value currVal = this._store.get(keyString);

		/**
		 * If the key not exist (value's length == 0) and the value to be put is empty (remove operation),
		 * return a response with response type NON_EXISTENT_KEY.
		 */
		if (currVal.value.length == 0 && val.length == 0)
		{
			return StoreMessage.createResponse(id, StoreMessage.StoreResponseType.NON_EXISTENT_KEY);
		}

		/**
		 * Origin value is empty, but the value to be put is not empty.
		 * Or origin value is not empty, but the value to be put is empty. (remove operation)
		 */
		this._store.put(keyString, new Value(val, currVal.version + 1));
		return StoreMessage.createResponse(id, StoreMessage.StoreResponseType.SUCCESSFUL);
	}
}
