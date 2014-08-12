package cn.paxos.rabbitsnail;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.apache.hadoop.conf.Configuration;

public class EntityManagerFactoryImpl implements EntityManagerFactory {

	// TODO Singlton or multiple ?
	private final EntityManager entityManager;

	public EntityManagerFactoryImpl(Configuration conf, List<String> managedClassNames) {
		this.entityManager = new EntityManagerImpl(conf, new Entities(managedClassNames));
	}

	@Override
	public EntityManager createEntityManager() {
		return entityManager;
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
