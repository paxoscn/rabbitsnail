package cn.paxos.rabbitsnail;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.FlushModeType;
import javax.persistence.Query;
import javax.persistence.TemporalType;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import cn.paxos.rabbitsnail.util.ByteArrayUtils;

public class QueryImpl implements Query {

	private static final Pattern QUERY_PATTERN_FINDING = Pattern.compile("^from[\\s]+([^\\s]+)(?:[\\s]+where[\\s]+(.+))?$");
	private static final Pattern QUERY_PATTERN_UPDATING = Pattern.compile("^update[\\s]+([^\\s]+)[\\s]+set[\\s]+(.+)[\\s]+where[\\s]+(.+)$");
	private static final Pattern SETTING_PATTERN = Pattern.compile("^(.+)=.+$");
	private static final Pattern CONDITION_PATTERN = Pattern.compile("^(.+)([=<>]).+$");

	private final EntityManagerImpl entityManagerImpl;
	private final String qlString;
	private final Map<Integer, Object> parameters;

	public QueryImpl(EntityManagerImpl entityManagerImpl, String qlString) {
		this.entityManagerImpl = entityManagerImpl;
		this.qlString = qlString;
		this.parameters = new HashMap<Integer, Object>();
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
					scan.setStartRow((byte[]) parameters.get(parameterIndex++));
				} else if (operator.equals("<")) {
					scan.setStopRow((byte[]) parameters.get(parameterIndex++));
				} else {
					throw new RuntimeException("Unsupported operator: " + operator);
				}
			}
		}
		List results = new LinkedList();
		HTable hTable = null;
		try {
			hTable = new HTable(entityManagerImpl.getConf(), entityDefinition.getTableName());
			ResultScanner rs = hTable.getScanner(scan);
			for(Result r : rs) {
				final Object entity = entityManagerImpl.readEntityFromResult(entityDefinition, r);
				results.add(entity);
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
		HTable hTable = null;
		try {
			hTable = new HTable(entityManagerImpl.getConf(), entityDefinition.getTableName());
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
		throw new UnsupportedOperationException();
	}

	@Override
	public Query setFirstResult(int startPosition) {
		throw new UnsupportedOperationException();
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

}
