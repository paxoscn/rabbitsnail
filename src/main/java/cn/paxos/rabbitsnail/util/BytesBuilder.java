package cn.paxos.rabbitsnail.util;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public class BytesBuilder {
	
	private final List<byte[]> byteArrayList = new LinkedList<>();
	
	public BytesBuilder add(String added) {
		byteArrayList.add(Bytes.toBytes(added));
		return this;
	}
	
	public BytesBuilder add(long added) {
		byteArrayList.add(Bytes.toBytes(added));
		return this;
	}
	
	public BytesBuilder add(int added) {
		byteArrayList.add(Bytes.toBytes(added));
		return this;
	}
	
	public byte[] toBytes() {
		byte[] bytes = new byte[0];
		for (byte[] nextByteArray : byteArrayList) {
			bytes = Bytes.add(bytes, nextByteArray);
		}
		return bytes;
	}

}
