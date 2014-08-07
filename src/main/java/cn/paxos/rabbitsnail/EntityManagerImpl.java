package cn.paxos.rabbitsnail;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import cn.paxos.rabbitsnail.util.ByteArrayUtils;

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
		byte[] id = entityDefinition.getId(entity);
		this.put(entityDefinition, entity, id, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T merge(T entity) {
		final Entity entityDefinition = entities.byType(entity.getClass());
		byte[] id = entityDefinition.getId(entity);
		while (true) {
			Object latestEntity = this.find(entity.getClass(), id);
			int oldVersion = (Integer) entityDefinition.getVersionColumn().get(latestEntity);
			boolean saved = this.put(entityDefinition, entity, id, oldVersion);
			if (saved) {
				return (T) this.find(entity.getClass(), id);
			} else {
				try { Thread.sleep(100); } catch (InterruptedException e) {}
			}
		}
	}

	@Override
	public void remove(Object entity) {
		final Entity entityDefinition = entities.byType(entity.getClass());
		byte[] id = entityDefinition.getId(entity);
		Delete delete = new Delete(id);
		HTable hTable = null;
		try {
			hTable = new HTable(conf, entityDefinition.getTableName());
			hTable.delete(delete);
		} catch (Exception e) {
			throw new RuntimeException("Error on deleting " + entity.getClass(), e);
		} finally {
			if (hTable != null)
				try {
					hTable.close();
				} catch (Exception e) {}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey) {
		final Entity entityDefinition = entities.byType(entityClass);
		Get get = new Get((byte[]) primaryKey);
		final String tableName = entityDefinition.getTableName();
		final Result result;
		HTable hTable = null;
		try {
			hTable = new HTable(conf, tableName);
			result = hTable.get(get);
			if (result.isEmpty()) {
				return null;
			}
			final Object entity = this.readEntityFromResult(entityDefinition, result);
			return (T) entity;
		} catch (Exception e) {
			throw new RuntimeException("Error on fetching " + entityClass + "#" + Bytes.toHex((byte[]) primaryKey), e);
		} finally {
			if (hTable != null)
				try {
					hTable.close();
				} catch (Exception e) {}
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
		return new QueryImpl(this, qlString.trim());
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

	Object readEntityFromResult(final Entity entityDefinition, final Result result) {
		final Object entity = entityDefinition.newInstance();
		entityDefinition.iterateColumns(entity, false, new ColumnIteratingCallback() {
			@SuppressWarnings("unchecked")
			@Override
			public void onColumn(Column column, Object value) {
				if (column == entityDefinition.getIdColumn()) {
					column.set(entity, result.getRow());
				} else {
					if (column.getAppended() == null) {
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
					} else {
						@SuppressWarnings("rawtypes")
						List list = (List) column.get(entity);
						Appended appended = column.getAppended();
						int itemIndex = 0;
						while (result.containsColumn(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(itemIndex))) {
							final Object item = appended.newInstance();
							list.add(item);
							String itemValue = Bytes.toString(result.getValue(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(itemIndex)));
							StringTokenizer st = new StringTokenizer(itemValue, "|");
							final Map<String, String> attributeMap = new HashMap<String, String>();
							while (st.hasMoreTokens()) {
								String keyAndValue = st.nextToken();
								String[] keyAndValueArray = keyAndValue.split("=");
								String attributeKey = keyAndValueArray[0];
								try {
									String attributeValue = URLDecoder.decode(keyAndValueArray[1], "UTF-8");
									attributeMap.put(attributeKey, attributeValue);
								} catch (UnsupportedEncodingException e) {
									throw new RuntimeException("Unknown error on encoding string: " + keyAndValueArray[1], e);
								}
							}
							appended.iterateColumns(item, false, new ColumnIteratingCallback() {
								@Override
								public void onColumn(Column column, Object value) {
									String readValueAsString = attributeMap.get(column.getColumn());
									if (readValueAsString == null) {
										return;
									}
									final Object readValue;
									Class<?> fieldType = column.getType();
									if (String.class.isAssignableFrom(fieldType)) {
										readValue = readValueAsString;
									} else if (Integer.class.isAssignableFrom(fieldType)
											|| int.class.isAssignableFrom(fieldType)) {
										readValue = Integer.parseInt(readValueAsString);
									} else if (Long.class.isAssignableFrom(fieldType)
											|| long.class.isAssignableFrom(fieldType)) {
										readValue = Long.parseLong(readValueAsString);
									} else if (Boolean.class.isAssignableFrom(fieldType)
											|| boolean.class.isAssignableFrom(fieldType)) {
										readValue = Boolean.parseBoolean(readValueAsString);
									} else if (BigDecimal.class.isAssignableFrom(fieldType)) {
										readValue = new BigDecimal(readValueAsString);
									} else if (Date.class.isAssignableFrom(fieldType)) {
										readValue = new Date(Long.parseLong(readValueAsString));
									} else {
										throw new RuntimeException("Unsupported column value type: " + fieldType + " of " + item.getClass() + "." + column.getField().getName());
									}
									column.set(item, readValue);
								}
							});
							itemIndex++;
						}
					}
				}
			}
		});
		return entity;
	}
	
	Configuration getConf() {
		return conf;
	}

	Entities getEntities() {
		return entities;
	}

	private boolean put(final Entity entityDefinition, final Object entity, byte[] id, Integer oldVersion) {
//		if (entityDefinition.getVersionColumn() == null) {
//			throw new RuntimeException("There is no version column for " + entity.getClass());
//		}
		final Put put = new Put(id);
		entityDefinition.iterateColumns(entity, true, new ColumnIteratingCallback() {
			@SuppressWarnings("rawtypes")
			@Override
			public void onColumn(Column column, Object value) {
				if (column == entityDefinition.getIdColumn()) {
					return;
				}
				if (value == null) {
					return;
				}
				if (column.getAppended() == null) {
					final byte[] columnValueAsBytes = ByteArrayUtils.toBytes(value);
					put.add(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumn()), columnValueAsBytes);
				} else {
					Appended appended = column.getAppended();
					int itemIndex = 0;
					for (Object item : (List) value) {
						final Map<String, Object> itemAttributes = new HashMap<String, Object>();
						appended.iterateColumns(item, true, new ColumnIteratingCallback() {
							@Override
							public void onColumn(Column column, Object value) {
								if (value == null) {
									return;
								}
								itemAttributes.put(column.getColumn(), value);
							}
						});
						StringBuilder itemSB = new StringBuilder();
						boolean started = false;
						for (String attributeName : itemAttributes.keySet()) {
							if (started) {
								itemSB.append("|");
							} else {
								started = true;
							}
							itemSB.append(attributeName);
							itemSB.append("=");
							try {
								itemSB.append(URLEncoder.encode(itemAttributes.get(attributeName).toString(), "UTF-8"));
							} catch (UnsupportedEncodingException e) {
								throw new RuntimeException("Unknown error on encoding string: " + itemAttributes.get(attributeName).toString(), e);
							}
						}
						put.add(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(itemIndex), Bytes.toBytes(itemSB.toString()));
						itemIndex++;
					}
				}
			}
		});
		HTable hTable = null;
		try {
			hTable = new HTable(conf, entityDefinition.getTableName());
			if (entityDefinition.getVersionColumn() != null) {
				put.add(
						Bytes.toBytes(entityDefinition.getVersionColumn().getColumnFamily()),
						Bytes.toBytes(entityDefinition.getVersionColumn().getColumn()),
						Bytes.toBytes(oldVersion == null ? 1 : oldVersion + 1));
				return hTable.checkAndPut(
						id,
						Bytes.toBytes(entityDefinition.getVersionColumn().getColumnFamily()),
						Bytes.toBytes(entityDefinition.getVersionColumn().getColumn()),
						oldVersion == null ? null : Bytes.toBytes(oldVersion),
						put);
			} else {
				hTable.put(put);
				return true;
			}
		} catch (Exception e) {
			throw new RuntimeException("Error on saving " + entity.getClass(), e);
		} finally {
			if (hTable != null)
				try {
					hTable.close();
				} catch (Exception e) {}
		}
	}

}
