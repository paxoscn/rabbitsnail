package cn.paxos.rabbitsnail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class Appended extends ColumnContainer {

	public Appended(Class<?> type) {
		super(type);
	}

	@Override
	protected void onGetter(Column column, Method getter) {
	}

	@Override
	protected void onField(Column column, Field field) {
	}

}
