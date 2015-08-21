package cn.paxos.rabbitsnail.util;

import java.math.BigDecimal;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class ByteArrayUtils {
	
	private ByteArrayUtils () {}
	
	public static byte[] toBytes(Object object) {
		if (object == null) {
			return null;
		}
		final byte[] bytes;
		if (object instanceof String) {
			bytes = Bytes.toBytes((String) object);
		} else if (object instanceof Integer) {
			bytes = Bytes.toBytes((Integer) object);
		} else if (object instanceof Long) {
			bytes = Bytes.toBytes((Long) object);
		} else if (object instanceof Double) {
			bytes = Bytes.toBytes((Double) object);
		} else if (object instanceof Boolean) {
			bytes = Bytes.toBytes((Boolean) object);
		} else if (object instanceof BigDecimal) {
			bytes = Bytes.toBytes((BigDecimal) object);
		} else if (object instanceof Date) {
			bytes = Bytes.toBytes((int) ((Date) object).getTime());
		} else if (object instanceof byte[]) {
			bytes = (byte[]) object;
		} else {
			throw new RuntimeException("Failed on casting to byte array: " + object);
		}
		return bytes;
	}
	
	public static Object fromBytes(Class<?> type, byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		final Object object;
		if (String.class.isAssignableFrom(type)) {
			object = Bytes.toString(bytes);
		} else if (Integer.class.isAssignableFrom(type)
				|| int.class.isAssignableFrom(type)) {
			object = Bytes.toInt(bytes);
		} else if (Long.class.isAssignableFrom(type)
				|| long.class.isAssignableFrom(type)) {
			object = Bytes.toLong(bytes);
		} else if (Double.class.isAssignableFrom(type)
				|| double.class.isAssignableFrom(type)) {
			object = Bytes.toDouble(bytes);
		} else if (Boolean.class.isAssignableFrom(type)
				|| boolean.class.isAssignableFrom(type)) {
			object = Bytes.toBoolean(bytes);
		} else if (BigDecimal.class.isAssignableFrom(type)) {
			object = Bytes.toBigDecimal(bytes);
		} else if (Date.class.isAssignableFrom(type)) {
			object = new Date(Bytes.toLong(bytes));
		} else if (byte[].class.isAssignableFrom(type)) {
			object = bytes;
		} else {
			throw new RuntimeException("Unsupported value type: " + type);
		}
		return object;
	}

	public static byte[] increaseOne(byte[] bytes) {
		for (int i = bytes.length - 1; i >= 0; i--) {
			byte b = bytes[i];
			if (b != (byte) 0xFF) {
				byte newByte = (byte) (b + 1);
				bytes[i] = newByte;
				return bytes;
			}
		}
		throw new RuntimeException("It is an all-1 array!");
	}
	
	public static void main(String[] args) {
		byte[] b = new byte[2];
		for (;;) {
			b = increaseOne(b);
			System.out.println(Bytes.toHex(b));
		}
	}

}
