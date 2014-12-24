package cn.paxos.rabbitsnail;

import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.ProviderUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class PersistenceProviderImpl implements PersistenceProvider {

	@Override
	public EntityManagerFactory createEntityManagerFactory(String emName,
			@SuppressWarnings("rawtypes") Map map) {
		throw new UnsupportedOperationException(
				"Unsupported factory method: createEntityManagerFactory(String emName, Map map). Use createContainerEntityManagerFactory(PersistenceUnitInfo info, Map map) instead.");
	}

	@Override
	public EntityManagerFactory createContainerEntityManagerFactory(
			PersistenceUnitInfo info, @SuppressWarnings("rawtypes") Map map) {
//		String hbaseZookeeperQuorum = (String) map.get(Constants.HBASE_ZOOKEEPER_QUORUM);
		Configuration conf = HBaseConfiguration.create();
//		conf.set(Constants.HBASE_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
		return new EntityManagerFactoryImpl(conf, info.getManagedClassNames());
	}

	@Override
	public void generateSchema(PersistenceUnitInfo info, @SuppressWarnings("rawtypes") Map map) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean generateSchema(String persistenceUnitName, @SuppressWarnings("rawtypes") Map map) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ProviderUtil getProviderUtil() {
		throw new UnsupportedOperationException();
	}

}
