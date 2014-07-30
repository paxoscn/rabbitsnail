package cn.paxos.rabbitsnail;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.apache.hadoop.conf.Configuration;

public class EntityManagerFactoryImpl implements EntityManagerFactory {
	
	private final Configuration conf;

	public EntityManagerFactoryImpl(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public EntityManager createEntityManager() {
		return new EntityManagerImpl(conf);
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
