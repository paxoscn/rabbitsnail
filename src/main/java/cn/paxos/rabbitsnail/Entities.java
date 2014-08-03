package cn.paxos.rabbitsnail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Entities {
	
	private final Map<Class<?>, Entity> entityMap = new HashMap<Class<?>, Entity>();

	public Entities(List<String> managedClassNames) {
		for (String managedClassName : managedClassNames) {
			final Class<?> entityType;
			try {
				entityType = Class.forName(managedClassName, true, Thread.currentThread().getContextClassLoader());
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

	public Entity byName(String entityTypeName) {
		for (Class<?> entityType : entityMap.keySet()) {
			if (entityType.getName().equals(entityTypeName)
					|| entityType.getSimpleName().equals(entityTypeName)) {
				return entityMap.get(entityType);
			}
		}
		throw new RuntimeException("Unmapped class: " + entityTypeName);
	}

}
