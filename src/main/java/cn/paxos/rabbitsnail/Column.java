package cn.paxos.rabbitsnail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class Column {

	private final String name;
	private String columnFamily;
	private String column;
	private Method getter;
	private Method setter;
	private Field field;
	private Appended appended;
	
	public Column(String name) {
		this.name = name;
	}

	public Object get(Object entity) {
		try {
			if (getter != null) {
				return getter.invoke(entity);
			} else {
				return field.get(entity);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error on getting " + field.getName() + " of " + entity.getClass() + " by " + getter, e);
		}
	}

	public void set(Object entity, Object value) {
		try {
			if (setter != null) {
				setter.invoke(entity, value);
			} else {
				field.set(entity, value);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error on setting " + field.getName() + " of " + entity.getClass() + " by " + getter, e);
		}
	}

	public Class<?> getType() {
		if (getter != null) {
			return getter.getReturnType();
		} else {
			return field.getType();
		}
	}
	
	public String getName() {
		return name;
	}
	public String getColumnFamily() {
		return columnFamily;
	}
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public Method getGetter() {
		return getter;
	}
	public void setGetter(Method getter) {
		this.getter = getter;
	}
	public Method getSetter() {
		return setter;
	}
	public void setSetter(Method setter) {
		this.setter = setter;
	}
	public Field getField() {
		return field;
	}
	public void setField(Field field) {
		this.field = field;
	}
	public Appended getAppended() {
		return appended;
	}
	public void setAppended(Appended appended) {
		this.appended = appended;
	}
	
}
