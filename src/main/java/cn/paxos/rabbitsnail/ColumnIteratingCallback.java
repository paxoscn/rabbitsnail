package cn.paxos.rabbitsnail;

public interface ColumnIteratingCallback {
	
	void onColumn(Column column, Object value);

}
