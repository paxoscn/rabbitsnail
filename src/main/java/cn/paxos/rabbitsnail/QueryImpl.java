package cn.paxos.rabbitsnail;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Parameter;
import javax.persistence.Query;
import javax.persistence.TemporalType;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import cn.paxos.rabbitsnail.util.ByteArrayUtils;

public class QueryImpl implements Query {

	private static final Pattern QUERY_PATTERN_FINDING = Pattern.compile("^from[\\s]+([^\\s]+)(?:[\\s]+where[\\s]+(.+))?$");
	private static final Pattern QUERY_PATTERN_UPDATING = Pattern.compile("^update[\\s]+([^\\s]+)[\\s]+set[\\s]+(.+)[\\s]+where[\\s]+(.+)$");
	private static final Pattern SETTING_PATTERN = Pattern.compile("^(.+)=.+$");
	private static final Pattern CONDITION_PATTERN = Pattern.compile("^(.+)([=<>]).+$");

	private static final Map<String, byte[]> INDEX_TO_ROW = new HashMap<String, byte[]>();
	
	private final EntityManagerImpl entityManagerImpl;
	private final String qlString;
	private final Map<Integer, Object> parameters;
	
	private int maxResult;
	private int startPosition;

	public QueryImpl(EntityManagerImpl entityManagerImpl, String qlString) {
		this.entityManagerImpl = entityManagerImpl;
		this.qlString = qlString;
		this.parameters = new HashMap<Integer, Object>();
		this.maxResult = -1;
		this.startPosition = -1;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List getResultList() {
		Matcher queryMatcher = QUERY_PATTERN_FINDING.matcher(qlString);
		if (!queryMatcher.find()) {
			throw new RuntimeException("Invalid query: " + qlString);
		}
		String entityTypeName = queryMatcher.group(1);
		String whereClause = queryMatcher.group(2);
		
		String startRowHex = null;
		int parameterIndex = 1;
		Entity entityDefinition = entityManagerImpl.getEntities().byName(entityTypeName);
		Scan scan = new Scan();
		if (whereClause != null) {
			String[] conditions = whereClause.split(" and ");
			for (String condition : conditions) {
				Matcher conditionMatcher = CONDITION_PATTERN.matcher(condition.trim());
				conditionMatcher.find();
				String operator = conditionMatcher.group(2);
				if (operator.equals(">")) {
					byte[] startRow = (byte[]) parameters.get(parameterIndex++);
					startRowHex = Bytes.toHex(startRow);
					scan.setStartRow(startRow);
				} else if (operator.equals("<")) {
					scan.setStopRow((byte[]) parameters.get(parameterIndex++));
				} else {
					throw new RuntimeException("Unsupported operator: " + operator);
				}
			}
		}
		boolean toCut = false;
		if (startPosition > -1) {
			String indexedRowKey = entityTypeName + "-" + startRowHex + "-" + (startPosition - 1);
			final byte[] indexedRow;
			synchronized (QueryImpl.class) {
				indexedRow = INDEX_TO_ROW.get(indexedRowKey);
			}
			if (indexedRow == null) {
				scan.setFilter(new PageFilter(startPosition + maxResult));
				toCut = true;
			} else {
				scan.setStartRow(ByteArrayUtils.increaseOne(indexedRow));
				scan.setFilter(new PageFilter(maxResult));
			}
		}
		List results = new LinkedList();
		HTableInterface hTable = null;
		try {
			hTable = entityManagerImpl.getTable(entityDefinition.getTableName());
			ResultScanner rs = hTable.getScanner(scan);
			int rowIndex = -1;
			Result lastResult = null;
			for(Result r : rs) {
				rowIndex++;
				lastResult = r;
				if (toCut && rowIndex < startPosition) {
					continue;
				}
				final Object entity = entityManagerImpl.readEntityFromResult(entityDefinition, r);
				results.add(entity);
			}
			if (startPosition > -1 && lastResult != null) {
				String indexedRowKey = entityTypeName + "-" + startRowHex + "-" + (startPosition + rowIndex);
				synchronized (QueryImpl.class) {
					INDEX_TO_ROW.put(indexedRowKey, lastResult.getRow());
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Error on querying: " + qlString, e);
		} finally {
			if (hTable != null)
				try {
					hTable.close();
				} catch (Exception e) {}
		}
		return results;
	}

	@Override
	public Object getSingleResult() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int executeUpdate() {
		Matcher queryMatcher = QUERY_PATTERN_UPDATING.matcher(qlString);
		if (!queryMatcher.find()) {
			throw new RuntimeException("Invalid query: " + qlString);
		}
		String entityTypeName = queryMatcher.group(1);
		String setClause = queryMatcher.group(2);
		String whereClause = queryMatcher.group(3);
		
		int parameterIndex = 1;
		Entity entityDefinition = entityManagerImpl.getEntities().byName(entityTypeName);

		List<String> setFields = new LinkedList<String>();
		String[] sets = setClause.split("\\,");
		for (String set : sets) {
			Matcher setMatcher = SETTING_PATTERN.matcher(set.trim());
			setMatcher.find();
			setFields.add(setMatcher.group(1).trim());
			parameterIndex++;
		}
		byte[] id = null;
		String checkingFieldName = null;
		byte[] checkingFieldValue = null;
		String[] conditions = whereClause.split(" and ");
		for (String condition : conditions) {
			Matcher conditionMatcher = CONDITION_PATTERN.matcher(condition.trim());
			conditionMatcher.find();
			String field = conditionMatcher.group(1).trim();
			if (field.equals(entityDefinition.getIdColumn().getName())) {
				id = (byte[]) parameters.get(parameterIndex);
			} else {
				checkingFieldName = field;
				checkingFieldValue = ByteArrayUtils.toBytes(parameters.get(parameterIndex));
			}
			parameterIndex++;
		}

		parameterIndex = 1;
		final Put put = new Put(id);
		for (String setField : setFields) {
			Column column = entityDefinition.getColumn(setField);
			final byte[] columnValueAsBytes = ByteArrayUtils.toBytes(parameters.get(parameterIndex));
			put.add(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumn()), columnValueAsBytes);
			parameterIndex++;
		}
		HTableInterface hTable = null;
		try {
			hTable = entityManagerImpl.getTable(entityDefinition.getTableName());
			if (checkingFieldName == null) {
				hTable.put(put);
				return 1;
			} else {
				Column checkingColumn = entityDefinition.getColumn(checkingFieldName);
				return hTable.checkAndPut(
						id,
						Bytes.toBytes(checkingColumn.getColumnFamily()),
						Bytes.toBytes(checkingColumn.getColumn()),
						checkingFieldValue,
						put) ? 1 : 0;
			}
		} catch (Exception e) {
			throw new RuntimeException("Error on querying: " + qlString, e);
		} finally {
			if (hTable != null)
				try {
					hTable.close();
				} catch (Exception e) {}
		}
	}

	@Override
	public Query setMaxResults(int maxResult) {
		this.maxResult = maxResult;
		return this;
	}

	@Override
	public Query setFirstResult(int startPosition) {
		this.startPosition = startPosition;
		return this;
	}

	@Override
	public Query setHint(String hintName, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setParameter(String name, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setParameter(String name, Date value, TemporalType temporalType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setParameter(String name, Calendar value,
			TemporalType temporalType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setParameter(int position, Object value) {
		parameters.put(position, value);
		return this;
	}

	@Override
	public Query setParameter(int position, Date value,
			TemporalType temporalType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setParameter(int position, Calendar value,
			TemporalType temporalType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setFlushMode(FlushModeType flushMode) {
		throw new UnsupportedOperationException();
	}
	
	public static void main(String[] args) {
		Matcher matcher = QUERY_PATTERN_FINDING.matcher("from User");
		while (matcher.find()) {
			for (int i = 0; i <= matcher.groupCount(); i++) {
				System.out.println(i + " " + matcher.group(i));
			}
		}
		matcher = QUERY_PATTERN_UPDATING.matcher("update User set name = ?, code = ? where id = ? and name = ?");
		while (matcher.find()) {
			for (int i = 0; i <= matcher.groupCount(); i++) {
				System.out.println(i + " " + matcher.group(i));
			}
		}
	}

	@Override
	public int getMaxResults() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getFirstResult() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Object> getHints() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Query setParameter(Parameter<T> param, T value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setParameter(Parameter<Calendar> param, Calendar value,
			TemporalType temporalType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setParameter(Parameter<Date> param, Date value,
			TemporalType temporalType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Parameter<?>> getParameters() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Parameter<?> getParameter(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Parameter<T> getParameter(String name, Class<T> type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Parameter<?> getParameter(int position) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Parameter<T> getParameter(int position, Class<T> type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isBound(Parameter<?> param) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T getParameterValue(Parameter<T> param) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getParameterValue(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getParameterValue(int position) {
		throw new UnsupportedOperationException();
	}

	@Override
	public FlushModeType getFlushMode() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setLockMode(LockModeType lockMode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public LockModeType getLockMode() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T unwrap(Class<T> cls) {
		throw new UnsupportedOperationException();
	}

}
