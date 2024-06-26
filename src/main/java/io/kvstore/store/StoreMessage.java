/**
 * StoreMessage.java
 * handles the serialization and deserialization of messages between the store and the client.
 */
package main.java.io.kvstore.store;

import java.util.Arrays;
import java.util.Random;

public class StoreMessage
{
	public enum StoreRequestType
	{
		PUT,
		GET,
		HEARTBEAT_REQ,
		UNKNOWN
	}

	public enum StoreResponseType
	{
		SUCCESSFUL,
		NON_EXISTENT_KEY,
		OUT_OF_SPACE,
		HEARTBEAT_ACK,
		UNRECOGNIZED_COMMAND
	}
	
	public static final int ID_BYTES = 16;
	public static final int COMMAND_BYTES = 1;
	public static final int KEY_BYTES = 32;
	public static final int VALUE_VER_BYTES = 2;
	public static final int VALUE_LEN_BYTES = 2;
	public static final int VALUE_MAX_BYTES = 15000;
	public static final int MIN_BYTES = ID_BYTES + COMMAND_BYTES;
	public static final int MAX_REQ_BYTES = MIN_BYTES + KEY_BYTES + VALUE_VER_BYTES + VALUE_LEN_BYTES + VALUE_MAX_BYTES;
	public static final int MAX_RES_BYTES = MIN_BYTES + VALUE_VER_BYTES + VALUE_LEN_BYTES + VALUE_MAX_BYTES;

	/**
	 * Extracts the ID from a message
	 * @param msg
	 * @return the ID
	 */
	public static byte[] id(byte[] msg)
	{
		return Arrays.copyOfRange(msg, 0, ID_BYTES);
	}

	/**
	 * Creates a random ID
	 * @return
	 */
	public static byte[] createId()
	{
		byte[] id = new byte[ID_BYTES];
		new Random().nextBytes(id);
		return id;
	}

	/**
	 * Extracts the request type from a message
	 * @param request
	 * @return the request type
	 */
	public static StoreRequestType storeRequestType(byte[] request)
	{
		switch (request[ID_BYTES])
		{
			case 0x01: return StoreRequestType.PUT;
			case 0x02: return StoreRequestType.GET;
			case 0x03: return StoreRequestType.HEARTBEAT_REQ;
			default: return StoreRequestType.UNKNOWN;
		}
	}

	/**
	 * Converts a request type to a byte
	 * @param request
	 * @return
	 */
	public static byte storeRequestType(StoreRequestType request)
	{
		switch (request)
		{
			case PUT: return 0x01;
			case GET: return 0x02;
			case HEARTBEAT_REQ: return 0x03;
			default: return 0x00;
		}
	}

	/**
	 * Extracts the response type from a message
	 * @param request
	 * @return
	 */
	public static StoreResponseType storeResponseType(byte[] request)
	{
		switch (request[ID_BYTES])
		{
			case 0x01: return StoreResponseType.SUCCESSFUL;
			case 0x02: return StoreResponseType.NON_EXISTENT_KEY;
			case 0x03: return StoreResponseType.OUT_OF_SPACE;
			case 0x04: return StoreResponseType.HEARTBEAT_ACK;
			default: return StoreResponseType.UNRECOGNIZED_COMMAND;
		}
	}

	/**
	 * Converts a response type to a byte
	 * @param response
	 * @return
	 */
	public static byte storeResponseType(StoreResponseType response)
	{
		switch (response)
		{
			case SUCCESSFUL: return 0x01;
			case NON_EXISTENT_KEY: return 0x02;
			case OUT_OF_SPACE: return 0x03;
			case HEARTBEAT_ACK: return 0x04;
			default: return 0x00;
		}
	}

	/**
	 * Extracts the key from a message
	 * @param request
	 * @return
	 */
	public static byte[] key(byte[] request)
	{
		return Arrays.copyOfRange(request, MIN_BYTES, MIN_BYTES + KEY_BYTES);
	}

	/**
	 * Extracts the value version from a request message
	 * @param request
	 * @return
	 */
	public static int requestValueVersion(byte[] request)
	{
		int firstByteIndex = MIN_BYTES + KEY_BYTES;
		int secondByteIndex = firstByteIndex + 1;
		return (request[firstByteIndex] & 0xFF) + ((request[secondByteIndex] << 8) & 0xFF00);
	}

	/**
	 * Extracts the value from a request message
	 * @param request
	 * @return
	 */
	public static byte[] requestValue(byte[] request)
	{
		if (request.length <= MIN_BYTES)
		{
			return new byte[0];
		}
		int firstByteIndex = MIN_BYTES + KEY_BYTES + VALUE_VER_BYTES;
		int secondByteIndex = firstByteIndex + 1;
		int length = (request[firstByteIndex] & 0xFF) + ((request[secondByteIndex] << 8) & 0xFF00);
		if (length == 0)
		{
			return new byte[0];
		}
		int start = MIN_BYTES + KEY_BYTES + VALUE_VER_BYTES + VALUE_LEN_BYTES;
		int end = start + length;
		return Arrays.copyOfRange(request, start, end);
	}

	/**
	 * Extracts the value version from a response message
	 * @param response
	 * @return
	 */
	public static int responseValueVersion(byte[] response)
	{
		int firstByteIndex = MIN_BYTES;
		int secondByteIndex = firstByteIndex + 1;
		return (response[firstByteIndex] & 0xFF) + ((response[secondByteIndex] << 8) & 0xFF00);
	}

	/**
	 * Extracts the value from a response message
	 * @param response
	 * @return
	 */
	public static byte[] responseValue(byte[] response)
	{
		if (response.length <= MIN_BYTES + VALUE_VER_BYTES + VALUE_LEN_BYTES)
		{
			return new byte[0];
		}
		int firstByteIndex = MIN_BYTES + VALUE_VER_BYTES;
		int secondByteIndex = firstByteIndex + 1;
		int length = (response[firstByteIndex] & 0xFF) + ((response[secondByteIndex] << 8) & 0xFF00);
		if (length == 0)
		{
			return new byte[0];
		}
		int start = MIN_BYTES + VALUE_VER_BYTES + VALUE_LEN_BYTES;
		int end = start + length;
		return Arrays.copyOfRange(response, start, end);
	}

	/**
	 * Creates a GET request
	 * @param id
	 * @param key
	 * @return
	 */
	public static byte[] createGetRequest(byte[] id, byte[] key)
	{
		return createRequest(createId(), StoreRequestType.GET, key, new byte[0], 0);
	}

	/**
	 * Creates a PUT request
	 * @param id
	 * @param key
	 * @param value
	 * @param version
	 * @return
	 */
	public static byte[] createPutRequest(byte[] id, byte[] key, byte[] value, int version)
	{
		return createRequest(createId(), StoreRequestType.PUT, key, value, version);
	}

	/**
	 * Creates a HEARTBEAT request
	 * @return
	 */
	public static byte[] createHeartbeatRequest()
	{
		return createRequest(createId(), StoreRequestType.HEARTBEAT_REQ, new byte[0], new byte[0], 0);
	}

	/**
	 * Creates a request message
	 * @param id
	 * @param request
	 * @param key
	 * @param value
	 * @param version
	 * @return
	 */
	public static byte[] createRequest(byte[] id, StoreRequestType request, byte[] key, byte[] value, int version)
	{
		int valueLength = Math.min(value.length, VALUE_MAX_BYTES);
		byte[] result = new byte[ID_BYTES + COMMAND_BYTES + KEY_BYTES + VALUE_VER_BYTES + VALUE_LEN_BYTES + valueLength];
		System.arraycopy(id, 0, result, 0, Math.min(id.length, ID_BYTES));
		result[ID_BYTES] = storeRequestType(request);
		System.arraycopy(key, 0, result, MIN_BYTES, Math.min(key.length, KEY_BYTES));
		result[ID_BYTES + COMMAND_BYTES + KEY_BYTES] = (byte) (version & 0xFF);
		result[ID_BYTES + COMMAND_BYTES + KEY_BYTES + 1] = (byte) ((version >> 8) & 0xFF);
		result[ID_BYTES + COMMAND_BYTES + KEY_BYTES + VALUE_VER_BYTES] = (byte) (valueLength & 0xFF);
		result[ID_BYTES + COMMAND_BYTES + KEY_BYTES + VALUE_VER_BYTES + 1] = (byte) ((valueLength >> 8) & 0xFF);
		System.arraycopy(value, 0, result, ID_BYTES + COMMAND_BYTES + KEY_BYTES + VALUE_VER_BYTES + VALUE_LEN_BYTES, valueLength);
		return result;
	}

	/**
	 * Create a response message
	 * @param id
	 * @param response
	 * @return
	 */
	public static byte[] createResponse(byte[] id, StoreResponseType response)
	{
		return createResponse(id, response, 0, new byte[0]);
	}

	/**
	 * Create a response message
	 * @param id
	 * @param response
	 * @param version
	 * @param value
	 * @return
	 */
	public static byte[] createResponse(byte[] id, StoreResponseType response, int version, byte[] value)
	{
		int valueLength = Math.min(value.length, VALUE_MAX_BYTES);
		byte[] result = new byte[ID_BYTES + COMMAND_BYTES + VALUE_VER_BYTES + VALUE_LEN_BYTES + valueLength];
		System.arraycopy(id, 0, result, 0, Math.min(id.length, ID_BYTES));
		result[ID_BYTES] = storeResponseType(response);
		result[ID_BYTES + COMMAND_BYTES] = (byte) (version & 0xFF);
		result[ID_BYTES + COMMAND_BYTES + 1] = (byte) ((version >> 8) & 0xFF);
		result[ID_BYTES + COMMAND_BYTES + VALUE_VER_BYTES] = (byte) (valueLength & 0xFF);
		result[ID_BYTES + COMMAND_BYTES + VALUE_VER_BYTES + 1] = (byte) ((valueLength >> 8) & 0xFF);
		System.arraycopy(value, 0, result, ID_BYTES + COMMAND_BYTES + VALUE_VER_BYTES + VALUE_LEN_BYTES, valueLength);
		return result;
	}
}
