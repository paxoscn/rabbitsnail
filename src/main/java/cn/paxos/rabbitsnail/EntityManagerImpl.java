package cn.paxos.rabbitsnail;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class EntityManagerImpl implements EntityManager {

	private final Configuration conf;
	private final Entities entities;

	public EntityManagerImpl(Configuration conf, Entities entities) {
		this.conf = conf;
		this.entities = entities;
	}

	@Override
	public void persist(final Object entity) {
		final Entity entityDefinition = entities.byType(entity.getClass());
		final byte[] id = entityDefinition.getId(entity);
		final Put put = new Put(id);
		entityDefinition.iterateColumns(entity, true, new ColumnIteratingCallback() {
			@Override
			public void onColumn(Column column, Object value) {
				if (column == entityDefinition.getIdColumn()) {
					return;
				}
				if (value == null) {
					return;
				}
				final byte[] columnValueAsBytes;
				if (value instanceof String) {
					columnValueAsBytes = Bytes.toBytes((String) value);
				} else if (value instanceof Integer) {
					columnValueAsBytes = Bytes.toBytes((Integer) value);
				} else if (value instanceof Long) {
					columnValueAsBytes = Bytes.toBytes((Long) value);
				} else if (value instanceof Boolean) {
					columnValueAsBytes = Bytes.toBytes((Boolean) value);
				} else if (value instanceof BigDecimal) {
					columnValueAsBytes = Bytes.toBytes((BigDecimal) value);
				} else if (value instanceof Date) {
					columnValueAsBytes = Bytes.toBytes((int) ((Date) value).getTime());
				} else {
					throw new RuntimeException("Unknown column value type: " + value + " from " + entity.getClass() + "." + column.getColumnFamily() + ":" + column.getColumn());
				}
				put.add(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumn()), columnValueAsBytes);
			}
		});
		put.add(
				Bytes.toBytes(entityDefinition.getVersionColumn().getColumnFamily()),
				Bytes.toBytes(entityDefinition.getVersionColumn().getColumn()),
				Bytes.toBytes(1));
		try (HTable hTable = new HTable(conf, entityDefinition.getTableName())) {
			hTable.checkAndPut(
					id,
					Bytes.toBytes(entityDefinition.getVersionColumn().getColumnFamily()),
					Bytes.toBytes(entityDefinition.getVersionColumn().getColumn()),
					null,
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
		final Entity entityDefinition = entities.byType(entityClass);
		Get get = new Get((byte[]) primaryKey);
		final String tableName = entityDefinition.getTableName();
		final Result result;
		try (HTable hTable = new HTable(conf, tableName)) {
			result = hTable.get(get);
			if (result.isEmpty()) {
				return null;
			}
			final Object entity = entityClass.newInstance();
			entityDefinition.iterateColumns(entity, false, new ColumnIteratingCallback() {
				@Override
				public void onColumn(Column column, Object value) {
					if (column == entityDefinition.getIdColumn()) {
						column.set(entity, result.getRow());
					} else {
						final byte[] columnValueAsBytes = result.getValue(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumn()));
						if (columnValueAsBytes == null) {
							return;
						}
						Class<?> fieldType = column.getType();
						final Object fieldValue;
						if (String.class.isAssignableFrom(fieldType)) {
							fieldValue = Bytes.toString(columnValueAsBytes);
						} else if (Integer.class.isAssignableFrom(fieldType)
								|| int.class.isAssignableFrom(fieldType)) {
							fieldValue = Bytes.toInt(columnValueAsBytes);
						} else if (Long.class.isAssignableFrom(fieldType)
								|| long.class.isAssignableFrom(fieldType)) {
							fieldValue = Bytes.toLong(columnValueAsBytes);
						} else if (Boolean.class.isAssignableFrom(fieldType)
								|| boolean.class.isAssignableFrom(fieldType)) {
							fieldValue = Bytes.toBoolean(columnValueAsBytes);
						} else if (BigDecimal.class.isAssignableFrom(fieldType)) {
							fieldValue = Bytes.toBigDecimal(columnValueAsBytes);
						} else if (Date.class.isAssignableFrom(fieldType)) {
							fieldValue = new Date(Bytes.toLong(columnValueAsBytes));
						} else {
							throw new RuntimeException("Unsupported column value type: " + fieldType + " of " + entity.getClass() + "." + column.getField().getName());
						}
						column.set(entity, fieldValue);
					}
				}
			});
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

}
