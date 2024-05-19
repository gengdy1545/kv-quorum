/**
 * StoreClient.java
 * StoreClient is a client for the StoreServer.
 * It sends requests to the server and waits for responses.
 */
package main.java.io.kvstore.store;

import main.java.io.kvstore.utils.Utils;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class StoreClient
{
	/**
	 * The port number of the store server.
	 */
	private final int _port;

	/**
	 * The number of attempts to send a request.
	 */
	private final int _attempts;

	/**
	 * The timeout in milliseconds for a response.
	 */
	private final int _timeoutMs;

	/**
	 * The socket for sending and receiving packets.
	 */
	private final DatagramSocket _socket;

	/**
	 * The executor for scheduling retry sends and timeout logic.
	 */
	private final ScheduledExecutorService _executor = Executors.newSingleThreadScheduledExecutor();

	/**
	 * The map of request keys to success callbacks.
	 */
	private final ConcurrentHashMap<String, Consumer<byte[]>> _onSuccess = new ConcurrentHashMap<>();

	/**
	 * The map of request keys to failure callbacks.
	 */
	private final ConcurrentHashMap<String, Consumer<byte[]>> _onFailure = new ConcurrentHashMap<>();

	public StoreClient(int port, int attempts, int timeoutMs) throws SocketException
	{
		this._port = port;
		this._attempts = attempts;
		this._timeoutMs = timeoutMs;
		this._socket = new DatagramSocket();
		this._socket.setReuseAddress(true);
	}

	/**
	 * Constructs a key for onSuccess and onFailure maps.
	 * @param addr
	 * @param port
	 * @param requestId
	 * @return
	 */
	private static String key(InetAddress addr, int port, byte[] requestId)
	{
		return Utils.hexString(addr.getAddress()) + Integer.toHexString(port) + Utils.hexString(requestId);
	}

	/**
	 * Send request to the specified address and port and set the onSuccess and onFailure callbacks.
	 * @param addr
	 * @param port
	 * @param data
	 * @param onSuccess
	 * @param onFailure
	 */
	public void send(InetAddress addr, int port, byte[] data, Consumer<byte[]> onSuccess, Consumer<byte[]> onFailure)
	{
		String key = key(addr, port, StoreMessage.id(data));
		this._onSuccess.put(key, onSuccess);
		this._onFailure.put(key, onFailure);
		
		DatagramPacket packet = new DatagramPacket(data, data.length, addr, port);
		
		// Send with retries
		for (int i = 0; i < this._attempts; i++) {
			this._executor.schedule(() -> {
				// If response received, onSuccess will be called and the key will be removed from the map
				if (this._onSuccess.containsKey(key)) {
					try {
						this._socket.send(packet);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}, (int) (i == 0 ? 0 : 250 * Math.pow(2, i)) , TimeUnit.MILLISECONDS);
		}
		 
		// Give up if no response received
		this._executor.schedule(() -> {
			Consumer<byte[]> thisOnFailure = this._onFailure.remove(key);
			if (thisOnFailure != null) {
				thisOnFailure.accept(data);
				this._onSuccess.remove(key);
			}
		}, _timeoutMs, TimeUnit.MILLISECONDS);
	}

	/**
	 * Receive responses from the store server and execute the onSuccess callback.
	 * @param executor
	 */
	public void receive(ExecutorService executor)
	{
		while (true)
		{
			byte[] response = new byte[StoreMessage.MAX_RES_BYTES];
			DatagramPacket packet = new DatagramPacket(response, response.length);
			try {
				this._socket.receive(packet);
				executor.execute(() -> {
					String key = key(packet.getAddress(), packet.getPort(), StoreMessage.id(response));
					Consumer<byte[]> onSuccess = this._onSuccess.remove(key);
					if (onSuccess != null) {
						onSuccess.accept(response); // call onSuccess callback
						this._onFailure.remove(key);
					}
				});
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
