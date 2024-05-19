/**
 * UdpClient.java
 *
 */
package test.java.io.kvstore;

import main.java.io.kvstore.utils.Utils;

import java.io.IOException;
import java.net.*;

public class UdpClient
{
	private final int timeout;
	private final int numRetries;
	
	public UdpClient(int retries, int timeout)
	{
		this.timeout = timeout;
		this.numRetries = retries;
	}
	
	public long sendAndReceive(InetAddress addr, int port, byte[] req, byte[] res) throws SocketException
	{
		DatagramPacket packet = new DatagramPacket(req, req.length, addr, port);
		DatagramSocket socket  = new DatagramSocket();
		socket.setSoTimeout(timeout);
		int retries = 0;
		while (retries <= numRetries) {
			Utils.print(retries);
			try {
				long start = System.currentTimeMillis();
				socket.send(packet);
				socket.receive(new DatagramPacket(res, res.length));
				long end = System.currentTimeMillis();
				socket.close();
				return end - start;
			} catch (SocketTimeoutException e) {
				retries++;
			} catch (IOException e2) {
				e2.printStackTrace();
				break;
			}
		}
		socket.close();
		return Long.MAX_VALUE;
	}
}
