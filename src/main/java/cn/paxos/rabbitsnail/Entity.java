package cn.paxos.rabbitsnail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.Version;

import org.apache.hadoop.hbase.util.Bytes;

public class Entity {

	private final Class<?> entityType;
	private final String tableName;
	private final Map<String, Column> columns;
	private final Column idColumn;
	private final Column versionColumn;

	public Entity(Class<?> entityType) {
		this.entityType = entityType;
		columns = new HashMap<>();
		if (!entityType.isAnnotationPresent(javax.persistence.Entity.class)) {
			throw new RuntimeException("There is no @Entity annotation on " + entityType);
		}
		tableName = extractTableName(entityType);
		Column idColumn_ = null;
		Column versionColumn_ = null;
		for (Method method : entityType.getMethods()) {
			if (method.getName().equals("getClass")
					|| !method.getName().startsWith("get")
					|| method.getParameterTypes().length > 0
					|| method.isAnnotationPresent(Transient.class)) {
				continue;
			}
			String fieldName = method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4);
			final Class<?> fieldType = method.getReturnType();
			Column column = new Column();
			columns.put(fieldName, column);
			if (method.isAnnotationPresent(Id.class)) {
				idColumn_ = column;
			}
			if (method.isAnnotationPresent(Version.class)) {
				versionColumn_ = column;
			}
			column.setGetter(method);
			ColumnFamily columnFamily = method.getAnnotation(ColumnFamily.class);
			if (columnFamily != null) {
				column.setColumnFamily(columnFamily.name());
			}
			column.setColumn(this.extractColumnName(method));
			try {
				column.setSetter(entityType.getMethod("set" + method.getName().substring(3), fieldType));
				column.setField(entityType.getDeclaredField(fieldName));
			} catch (Exception e) {
				throw new RuntimeException("There is no setter for " + entityType + "." + fieldName);
			}
		}
		for (Field field : entityType.getDeclaredFields()) {
			if (field.isAnnotationPresent(Transient.class)) {
				continue;
			}
			String fieldName = field.getName();
			Column column = columns.get(fieldName);
			if (column == null) {
				column = new Column();
				columns.put(fieldName, column);
			}
			if (field.isAnnotationPresent(Id.class)) {
				idColumn_ = column;
			}
			if (field.isAnnotationPresent(Version.class)) {
				versionColumn_ = column;
			}
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
		if (idColumn_ == null) {
			throw new RuntimeException("There is no id for " + entityType);
		}
		if (versionColumn_ == null) {
			throw new RuntimeException("There is no version column for " + entityType);
		}
		idColumn = idColumn_;
		versionColumn = versionColumn_;
	}

	public Object newInstance() {
		try {
			return entityType.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Error on initializing " + entityType, e);
		}
	}

	public byte[] getId(Object entity) {
		return (byte[]) idColumn.get(entity);
	}

	public void iterateColumns(Object entity, boolean valueNeeded, ColumnIteratingCallback columnIteratingCallback) {
		for (Column column : columns.values()) {
			columnIteratingCallback.onColumn(column, valueNeeded ? column.get(entity) : Void.class);
		}
	}
	
	public String getTableName() {
		return tableName;
	}

	public Column getIdColumn() {
		return idColumn;
	}

	public Column getVersionColumn() {
		return versionColumn;
	}

	private static String extractTableName(Class<?> entityClass) {
		final String tableName;
		Table table = entityClass.getAnnotation(Table.class);
		if (table == null) {
			tableName = entityClass.getSimpleName().toLowerCase();
		} else {
			tableName = table.name();
		}
		return tableName;
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

}
