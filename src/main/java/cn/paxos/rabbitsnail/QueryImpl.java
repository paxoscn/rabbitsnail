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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class QueryImpl implements Query {

	private static final Pattern QUERY_PATTERN = Pattern.compile("^from[\\s]+([^\\s]+)(?:[\\s]+where[\\s]+(.+))?$");
	private static final Pattern CONDITION_PATTERN = Pattern.compile("^.+([=<>]).+$");

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
		Matcher queryMatcher = QUERY_PATTERN.matcher(qlString.trim());
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
				String operator = conditionMatcher.group(1);
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
		throw new UnsupportedOperationException();
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
		Matcher matcher = QUERY_PATTERN.matcher("from User".trim());
		while (matcher.find()) {
			for (int i = 0; i <= matcher.groupCount(); i++) {
				System.out.println(i + " " + matcher.group(i));
			}
		}
	}

}
