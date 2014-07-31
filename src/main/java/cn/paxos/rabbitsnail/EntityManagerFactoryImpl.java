package cn.paxos.rabbitsnail;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.apache.hadoop.conf.Configuration;

public class EntityManagerFactoryImpl implements EntityManagerFactory {

	private final Configuration conf;
	private final Entities entities;

	public EntityManagerFactoryImpl(Configuration conf, List<String> managedClassNames) {
		this.conf = conf;
		this.entities = new Entities(managedClassNames);
	}

	@Override
	public EntityManager createEntityManager() {
		return new EntityManagerImpl(conf, entities);
	}

	@Override
	public EntityManager createEntityManager(@SuppressWarnings("rawtypes") Map map) {
		throw new UnsupportedOperationException(
				"Unsupported factory method: createEntityManager(Map map). Use createEntityManager() instead.");
	}

	@Override
	public void close() {
	}

	@Override
	public boolean isOpen() {
		return true;
	}

}
