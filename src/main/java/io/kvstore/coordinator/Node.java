/**
 * Node.java
 * This class represents a node in the system.
 */
package main.java.io.kvstore.coordinator;

import java.net.InetAddress;

public class Node {
	/**
	 * The address of the node.
	 */
	private InetAddress _address;

	/**
	 * The port number for the coordinator server.
	 */
	private int _coordinatorPort;

	/**
	 * The port number for the store server.
	 */
	private int _storePort;

	/**
	 * Whether the node is available.
	 */
	private boolean _available;
	
	public Node(InetAddress addr, int coordinatorPort, int storePort, boolean localhost)
	{
		this._address = addr;
		this._coordinatorPort = coordinatorPort;
		this._storePort = storePort;
		this._available = false;
	}
	
	public InetAddress getAddress() { return this._address; }

	public int getCoordinatorPort() { return this._coordinatorPort; }

	public int getStorePort() { return this._storePort; }
	
	public boolean getAvailable() { return this._available; }

	public void setAvailable(boolean val) { this._available = val;}
}
