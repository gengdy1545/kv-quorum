package main.java.io.kvstore.utils;

import javax.xml.bind.DatatypeConverter;

public class Utils {

	private Utils() {}
	
	public static String hexString(byte[] bytes)
	{
		return DatatypeConverter.printHexBinary(bytes);
	}

	public static void print(Object o)
	{
		System.out.println(o);
	}

}
