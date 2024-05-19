package main.java.io.kvstore;

import main.java.io.kvstore.coordinator.Coordinator;
import main.java.io.kvstore.coordinator.Monitor;
import main.java.io.kvstore.coordinator.Node;
import main.java.io.kvstore.store.Store;
import main.java.io.kvstore.store.StoreClient;
import main.java.io.kvstore.utils.Utils;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Runner
{
	public static void main(String[] args) throws Exception
	{
		ExecutorService executor = Executors.newCachedThreadPool();

		int numNodes = Config.PORT_COORDINATOR.length;
		Node[] nodes = new Node[numNodes];
		for (int i = 0; i < numNodes; i++)
		{
			InetAddress addr = InetAddress.getLocalHost();
			boolean isLocalhost = addr.equals(InetAddress.getLocalHost());
			nodes[i] = new Node(addr, Config.PORT_COORDINATOR[i], Config.PORT_STORE[i], isLocalhost);
		}

		for (int i = 0; i < numNodes; i++)
		{
			final int coordinatorPort = Config.PORT_COORDINATOR[i];
			final int storePort = Config.PORT_STORE[i];

			// start store service
			Store store = new Store(storePort);
			new Thread(() -> store.serve(executor)).start();
			Utils.print("Store serving on port " + storePort + "...");

			// create store client
			StoreClient client = new StoreClient(storePort, 5, 10000);
			new Thread(() -> client.receive(executor)).start();

			// start coordinator service
			Monitor monitor = new Monitor(nodes, client);
			Coordinator coordinator = new Coordinator(coordinatorPort, monitor, client);
			new Thread(() -> coordinator.serve(executor)).start();
			Utils.print("Coordinator serving on port " + coordinatorPort + "...");
		}
	}

}
