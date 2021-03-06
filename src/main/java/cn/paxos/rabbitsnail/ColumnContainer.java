package cn.paxos.rabbitsnail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Transient;

public abstract class ColumnContainer {

	private final Class<?> type;
	private final Map<String, Column> columns;

	public ColumnContainer(Class<?> type) {
		this.type = type;
		this.columns = new HashMap<String, Column>();
		Set<String> transientFields = new HashSet<String>();
		for (Method method : type.getMethods()) {
			if (method.getName().equals("getClass")
					|| (!method.getName().startsWith("get"))
					|| method.getParameterTypes().length > 0
					|| method.isAnnotationPresent(Transient.class)) {
				if (method.isAnnotationPresent(Transient.class)) {
					transientFields.add(method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4));
				}
				continue;
			}
			String fieldName = method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4);
			final Class<?> fieldType = method.getReturnType();
			Column column = new Column(fieldName);
			columns.put(fieldName, column);
			this.onGetter(column, method);
			column.setGetter(method);
			ColumnFamily columnFamily = method.getAnnotation(ColumnFamily.class);
			if (columnFamily != null) {
				column.setColumnFamily(columnFamily.name());
			}
			column.setColumn(this.extractColumnName(method));
			try {
				column.setSetter(type.getMethod("set" + method.getName().substring(3), fieldType));
				column.setField(searchDeclaredField(type, fieldName));
			} catch (Exception e) {
				throw new RuntimeException("There is no setter for " + type + "." + fieldName);
			}
		}
		for (Field field : searchDeclaredFields(type)) {
			if (field.isAnnotationPresent(Transient.class)
					|| transientFields.contains(field.getName())) {
				continue;
			}
			String fieldName = field.getName();
			Column column = columns.get(fieldName);
			if (column == null) {
				column = new Column(fieldName);
				columns.put(fieldName, column);
			}
			this.onField(column, field);
			field.setAccessible(true);
			column.setField(field);
			ColumnFamily columnFamily = field.getAnnotation(ColumnFamily.class);
			if (columnFamily != null) {
				column.setColumnFamily(columnFamily.name());
			}
			if (column.getColumn() == null) {
				column.setColumn(this.extractColumnName(field));
			}
		}
	}

	public final Object newInstance() {
		try {
			return this.getType().newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Error on initializing " + this.getType(), e);
		}
	}

	public final void iterateColumns(Object columnContainer, boolean valueNeeded, ColumnIteratingCallback columnIteratingCallback) {
		for (Column column : this.getColumns().values()) {
			columnIteratingCallback.onColumn(column, valueNeeded ? column.get(columnContainer) : Void.class);
		}
	}

	public Column getColumn(String fieldName) {
		return columns.get(fieldName);
	}

	protected abstract void onGetter(Column column, Method getter);

	protected abstract void onField(Column column, Field field);

	protected final Class<?> getType() {
		return type;
	}

	public final Map<String, Column> getColumns() {
		return columns;
	}

	private String extractColumnName(Method method) {
		final String columnName;
		javax.persistence.Column column = method.getAnnotation(javax.persistence.Column.class);
		if (column == null) {
			columnName = null;
		} else {
			columnName = column.name();
		}
		return columnName;
	}

	private String extractColumnName(Field field) {
		final String columnName;
		javax.persistence.Column column = field.getAnnotation(javax.persistence.Column.class);
		if (column == null) {
			columnName = field.getName();
		} else {
			columnName = column.name();
		}
		return columnName;
	}

	private Field searchDeclaredField(Class<?> searchingType, String fieldName) {
		try {
			return searchingType.getDeclaredField(fieldName);
		} catch (Exception e) {
			return searchDeclaredField(searchingType.getSuperclass(), fieldName);
		}
	}

	private List<Field> searchDeclaredFields(Class<?> searchingType) {
		List<Field> fields = new LinkedList<Field>();
		while (!searchingType.equals(Object.class)) {
			for (Field field : searchingType.getDeclaredFields()) {
				fields.add(field);
			}
			searchingType = searchingType.getSuperclass();
		}
		return fields;
	}

}
