/**
 * Monitor.java
 * Monitor the nodes in the system.
 */
package main.java.io.kvstore.coordinator;

import main.java.io.kvstore.Config;
import main.java.io.kvstore.store.StoreClient;
import main.java.io.kvstore.store.StoreMessage;

import java.math.BigInteger;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Monitor
{
	/**
	 * The nodes in the system.
	 */
	private final Node[] _nodes;


	/**
	 * The hashing algorithm.
	 */
	private final MessageDigest _digest;

	public Monitor(Node[] nodes, StoreClient client) throws SocketException, NoSuchAlgorithmException
	{
		this._nodes = nodes;
		this._digest = MessageDigest.getInstance(Config.HASHING_ALGORITHM);
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
				() -> {
					for (Node node : this._nodes) {
						client.send(node.getAddress(), node.getStorePort(), StoreMessage.createHeartbeatRequest(),
								res -> node.setAvailable(true), req -> node.setAvailable(false));
					}
				}, 0, 1, TimeUnit.MINUTES);
	}

	/**
	 * Get the id of the node that should store the data.
	 * @param data
	 * @return
	 */
	public int getId(byte[] data)
	{
		this._digest.update(data);
		BigInteger bigInt = new BigInteger(this._digest.digest());
		return bigInt.intValue() & Integer.MAX_VALUE % this._nodes.length;
	}
	
	/*
	 * Gets all the successors for this key, until numAvailable available nodes are retrieved.
	 */
	public Set<Node> getSuccessors(byte[] key, int numAvailable)
	{
		int id = getId(key);
		if (id < 0 || numAvailable < 1 || this._nodes.length < 1)
		{
			return new HashSet<Node>();
		}
		HashSet<Node> successors = new HashSet<>();
		int availableNodes = 0;
		/**
		 * ToDo:
		 * Issue: If the number of available nodes is less than numAvailable,
		 * the function will be stuck in an infinite loop.
		 * And repeatedly add the same node to the successors possibly.
		 */
		while (availableNodes < numAvailable)
		{
			Node node = this._nodes[id % this._nodes.length];
			successors.add(node);
			if (node.getAvailable())
			{
				availableNodes++;
			}
			id++;
		}
		return successors;
	}
}
