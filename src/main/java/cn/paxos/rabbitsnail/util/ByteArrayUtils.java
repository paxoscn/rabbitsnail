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
		} else if (object instanceof Boolean) {
			bytes = Bytes.toBytes((Boolean) object);
		} else if (object instanceof BigDecimal) {
			bytes = Bytes.toBytes((BigDecimal) object);
		} else if (object instanceof Date) {
			bytes = Bytes.toBytes((int) ((Date) object).getTime());
		} else {
			throw new RuntimeException("Failed on casting to byte array: " + object);
		}
		return bytes;
	}

}
