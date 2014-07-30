package cn.paxos.rabbitsnail;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.Id;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import javax.persistence.Table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class EntityManagerImpl implements EntityManager {

	private final Configuration conf;

	public EntityManagerImpl(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void persist(Object entity) {
		final String tableName = this.extractTableName(entity.getClass());
		
		Method idGetter = null;
		outter:
		for (Method method : entity.getClass().getMethods()) {
			if (method.isAnnotationPresent(Id.class)) {
				try {
					idGetter = method;
					break outter;
				} catch (Exception e) {
					throw new RuntimeException("Error on finding id of " + entity.getClass(), e);
				}
			}
		}
		if (idGetter == null) {
			throw new RuntimeException("There is no id for " + entity.getClass());
		}
		
		final byte[] id;
		try {
			id = (byte[]) idGetter.invoke(entity);
		} catch (Exception e) {
			throw new RuntimeException("Error on fetching id of " + entity.getClass() + " by " + idGetter, e);
		}

		Put put = new Put(id);
		for (Method method : entity.getClass().getMethods()) {
			if (method.equals(idGetter)
					|| method.getName().equals("getClass")
					|| !method.getName().startsWith("get")
					|| method.getParameterTypes().length > 0) {
				continue;
			}
			
			ColumnFamily columnFamily = method.getAnnotation(ColumnFamily.class);
			String columnFamilyName = columnFamily.name();

			final String columnName = this.extractColumnName(method);
			
			final Object columnValue;
			try {
				columnValue = method.invoke(entity);
			} catch (Exception e) {
				throw new RuntimeException("Error on fetching column of " + entity.getClass() + " by " + method, e);
			}
			if (columnValue == null) {
				continue;
			}
			final byte[] columnValueAsBytes;
			if (columnValue instanceof String) {
				columnValueAsBytes = Bytes.toBytes((String) columnValue);
			} else if (columnValue instanceof Integer) {
				columnValueAsBytes = Bytes.toBytes((Integer) columnValue);
			} else if (columnValue instanceof Long) {
				columnValueAsBytes = Bytes.toBytes((Long) columnValue);
			} else if (columnValue instanceof Boolean) {
				columnValueAsBytes = Bytes.toBytes((Boolean) columnValue);
			} else if (columnValue instanceof BigDecimal) {
				columnValueAsBytes = Bytes.toBytes((BigDecimal) columnValue);
			} else if (columnValue instanceof Date) {
				columnValueAsBytes = Bytes.toBytes((int) ((Date) columnValue).getTime());
			} else {
				throw new RuntimeException("Unknown column value type: " + columnValue + " from " + entity.getClass() + "." + method);
			}
			
			put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), columnValueAsBytes);
		}

		ExistenceFlag existenceFlag = entity.getClass().getAnnotation(ExistenceFlag.class);
		try (HTable hTable = new HTable(conf, tableName)) {
			hTable.checkAndPut(
					id,
					Bytes.toBytes(existenceFlag.family()),
					Bytes.toBytes(existenceFlag.column()),
					existenceFlag.matcher().newInstance().getExpectedValue(),
					put);
		} catch (Exception e) {
			throw new RuntimeException("Error on persisting " + entity.getClass(), e);
		}
	}

	@Override
	public <T> T merge(T entity) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove(Object entity) {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey) {
		final String tableName = this.extractTableName(entityClass);
		
		Get get = new Get((byte[]) primaryKey);
		final Result result;
		try (HTable hTable = new HTable(conf, tableName)) {
			result = hTable.get(get);
			if (result.isEmpty()) {
				return null;
			}
			
			Object entity = entityClass.newInstance();

			for (Method method : entityClass.getMethods()) {
				if (method.getName().equals("getClass")
						|| !method.getName().startsWith("get")
						|| method.getParameterTypes().length > 0) {
					continue;
				}

				final Class<?> fieldType = method.getReturnType();
				
				Method setter = entityClass.getMethod("set" + method.getName().substring(3), fieldType);
				
				if (method.isAnnotationPresent(Id.class)) {
					setter.invoke(entity, result.getRow());
				} else {
					ColumnFamily columnFamily = method.getAnnotation(ColumnFamily.class);
					String columnFamilyName = columnFamily.name();

					final String columnName = this.extractColumnName(method);

					final byte[] columnValueAsBytes = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
					if (columnValueAsBytes == null) {
						continue;
					}
					
					final Object fieldValue;
					if (String.class.isAssignableFrom(fieldType)) {
						fieldValue = Bytes.toString(columnValueAsBytes);
					} else if (Integer.class.isAssignableFrom(fieldType)) {
						fieldValue = Bytes.toInt(columnValueAsBytes);
					} else if (Long.class.isAssignableFrom(fieldType)) {
						fieldValue = Bytes.toLong(columnValueAsBytes);
					} else if (Boolean.class.isAssignableFrom(fieldType)) {
						fieldValue = Bytes.toBoolean(columnValueAsBytes);
					} else if (BigDecimal.class.isAssignableFrom(fieldType)) {
						fieldValue = Bytes.toBigDecimal(columnValueAsBytes);
					} else if (Date.class.isAssignableFrom(fieldType)) {
						fieldValue = new Date(Bytes.toLong(columnValueAsBytes));
					} else {
						throw new RuntimeException("Unsupported column value type: " + fieldType + " of " + entity.getClass() + "." + method);
					}

					setter.invoke(entity, fieldValue);
				}
			}
			
			return (T) entity;
		} catch (Exception e) {
			throw new RuntimeException("Error on fetching " + entityClass + "#" + Bytes.toHex((byte[]) primaryKey), e);
		}
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object primaryKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flush() {
	}

	@Override
	public void setFlushMode(FlushModeType flushMode) {
	}

	@Override
	public FlushModeType getFlushMode() {
		return FlushModeType.AUTO;
	}

	@Override
	public void lock(Object entity, LockModeType lockMode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void refresh(Object entity) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean contains(Object entity) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query createQuery(String qlString) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query createNamedQuery(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query createNativeQuery(String sqlString) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query createNativeQuery(String sqlString, @SuppressWarnings("rawtypes") Class resultClass) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query createNativeQuery(String sqlString, String resultSetMapping) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void joinTransaction() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getDelegate() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public EntityTransaction getTransaction() {
		throw new UnsupportedOperationException();
	}
	
	private String extractTableName(Class<?> entityClass) {
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
		Column column = method.getAnnotation(Column.class);
		if (column == null) {
			columnName = method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4);
		} else {
			columnName = column.name();
		}
		return columnName;
	}

}
