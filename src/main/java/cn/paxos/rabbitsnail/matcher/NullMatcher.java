package cn.paxos.rabbitsnail.matcher;

public class NullMatcher implements ColumnValueMatcher {

	@Override
	public byte[] getExpectedValue() {
		return null;
	}

}
