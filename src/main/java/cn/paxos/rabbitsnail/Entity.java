package cn.paxos.rabbitsnail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;

import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Version;

public class Entity extends ColumnContainer {

	private final String tableName;
	
	private Column idColumn;
	private Column versionColumn;

	public Entity(Class<?> type) {
		super(type);
		if (!type.isAnnotationPresent(javax.persistence.Entity.class)) {
			throw new RuntimeException("There is no @Entity annotation on " + type);
		}
		tableName = extractTableName(type);
		if (idColumn == null) {
			throw new RuntimeException("There is no id for " + type);
		}
		if (versionColumn == null) {
			throw new RuntimeException("There is no version column for " + type);
		}
	}

	public byte[] getId(Object entity) {
		return (byte[]) idColumn.get(entity);
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

	@Override
	protected void onGetter(Column column, Method getter) {
		if (getter.isAnnotationPresent(Id.class)) {
			idColumn = column;
		}
		if (getter.isAnnotationPresent(Version.class)) {
			versionColumn = column;
		}
		if (getter.isAnnotationPresent(OneToMany.class)) {
			column.setAppended(new Appended((Class<?>) ((ParameterizedType) getter.getGenericReturnType()).getActualTypeArguments()[0]));
		}
	}

	@Override
	protected void onField(Column column, Field field) {
		if (field.isAnnotationPresent(Id.class)) {
			idColumn = column;
		}
		if (field.isAnnotationPresent(Version.class)) {
			versionColumn = column;
		}
		if (field.isAnnotationPresent(OneToMany.class)) {
			column.setAppended(new Appended((Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0]));
		}
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

}
