package cn.paxos.rabbitsnail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.OperationWithAttributes;

public class Entities {
	
	private final Map<Class<?>, Entity> entityMap = new HashMap<>();

	public Entities(List<String> managedClassNames) {
		for (String managedClassName : managedClassNames) {
			final Class<?> entityType;
			try {
				entityType = Class.forName(managedClassName);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Failed to load " + managedClassName, e);
			}
			entityMap.put(entityType, new Entity(entityType));
		}
	}

	public Entity byType(Class<?> entityType) {
		Entity entity = entityMap.get(entityType);
		if (entity == null) {
			throw new RuntimeException("Unmapped class: " + entityType);
		}
		return entity;
	}

}
