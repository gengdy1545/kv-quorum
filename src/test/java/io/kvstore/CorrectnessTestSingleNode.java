package test.java.io.kvstore;

import main.java.io.kvstore.Config;
import main.java.io.kvstore.utils.Message;
import main.java.io.kvstore.utils.Utils;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class CorrectnessTestSingleNode {
    private static int numKeys = 1;

    public static void main(String[] args) throws SocketException, UnknownHostException {
        InetAddress addr = InetAddress.getByName("localhost");

        for (int i = 0; i < Config.PORT_COORDINATOR.length; i++) {
            int portCoordinator = Config.PORT_COORDINATOR[i];
            int portStore = Config.PORT_STORE[i];

            Utils.print("Testing node with Coordinator port " + portCoordinator + " and Store port " + portStore);

            ArrayList<byte[]> randomKeys = new ArrayList<>();
            for (int j = 0; j < numKeys; j++) {
                randomKeys.add(TestHelper.createRandomKey());
            }

            UdpClient client = new UdpClient(3, 1250);

            // PUTS!
            for (byte[] key : randomKeys) {
                byte[] val = ("This is a value for " + Utils.hexString(key)).getBytes();
                byte[] request = TestHelper.createPutRequest(key, val);
                byte[] response = new byte[Message.MIN_BYTES];

                long responseTime = client.sendAndReceive(addr, portCoordinator, request, response);

                if (responseTime < Long.MAX_VALUE) {
                    // Utils.print("PUT response: " + Message.code(response) + " " + responseTime);
                    Utils.print("put: " + key + "->" + val);
                } else {
                    Utils.print("PUT failed (" + addr.getHostName() + ":" + portCoordinator + ")");
                }
            }

            // GETS!
            for (byte[] key : randomKeys) {
                String val = "This is a value for " + Utils.hexString(key);
                byte[] request = TestHelper.createGetRequest(key);
                byte[] response = new byte[Message.RES_MAX_BYTES];

                long responseTime = client.sendAndReceive(addr, portCoordinator, request, response);

                if (responseTime < Long.MAX_VALUE) {
                    // Utils.print("GET response: " + Message.code(response) + " Correct value? " + val.equals(new String(TestHelper.getValue(response))) + " " + responseTime);
                    Utils.print("get: " + key + "->" + new String(TestHelper.getValue(response)) + "// Correct value is " + val + " " + val.equals(new String(TestHelper.getValue(response))));
                } else {
                    Utils.print("GET failed (" + addr.getHostName() + ":" + portCoordinator + ")");
                }
            }

            // REMOVES!
            for (byte[] key : randomKeys) {
                byte[] request = TestHelper.createRemoveRequest(key);
                byte[] response = new byte[Message.MIN_BYTES];

                long responseTime = client.sendAndReceive(addr, portCoordinator, request, response);

                if (responseTime < Long.MAX_VALUE) {
                    Utils.print("remove: " + key);
                    // Utils.print("REMOVE response: " + Message.code(response) + " " + responseTime);
                } else {
                    Utils.print("REMOVE failed (" + addr.getHostName() + ":" + portCoordinator + ")");
                }
            }

            // GETS!
            for (byte[] key : randomKeys) {
                byte[] request = TestHelper.createGetRequest(key);
                byte[] response = new byte[Message.MIN_BYTES];

                long responseTime = client.sendAndReceive(addr, portCoordinator, request, response);

                if (responseTime < Long.MAX_VALUE) {
                    // Utils.print("GET response: " + Message.code(response) + " " + responseTime);
                    Utils.print("get: " + key + "->" + Message.code(response));
                } else {
                    Utils.print("GET failed (" + addr.getHostName() + ":" + portCoordinator + ")");
                }
            }

            Utils.print("");
        }
    }

}