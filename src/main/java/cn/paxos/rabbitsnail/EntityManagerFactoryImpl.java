package cn.paxos.rabbitsnail;

import java.util.List;
import java.util.Map;

import javax.persistence.Cache;
import javax.persistence.EntityGraph;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.Query;
import javax.persistence.SynchronizationType;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;

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

	@Override
	public EntityManager createEntityManager(
			SynchronizationType synchronizationType) {
		throw new UnsupportedOperationException(
				"Unsupported factory method: createEntityManager(SynchronizationType synchronizationType). Use createEntityManager() instead.");
	}

	@Override
	public EntityManager createEntityManager(
			SynchronizationType synchronizationType, @SuppressWarnings("rawtypes") Map map) {
		throw new UnsupportedOperationException(
				"Unsupported factory method: createEntityManager(SynchronizationType synchronizationType, Map map). Use createEntityManager() instead.");
	}

	@Override
	public CriteriaBuilder getCriteriaBuilder() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Metamodel getMetamodel() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Object> getProperties() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Cache getCache() {
		throw new UnsupportedOperationException();
	}

	@Override
	public PersistenceUnitUtil getPersistenceUnitUtil() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addNamedQuery(String name, Query query) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T unwrap(Class<T> cls) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> void addNamedEntityGraph(String graphName,
			EntityGraph<T> entityGraph) {
		throw new UnsupportedOperationException();
	}

}
