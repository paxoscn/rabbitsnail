package cn.paxos.rabbitsnail.sample.helloworld.repository;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import cn.paxos.rabbitsnail.sample.helloworld.entity.User;
import cn.paxos.rabbitsnail.util.BytesBuilder;

public class UserRepository {

	private EntityManager entityManager;

	public void persistUser(User user) {
		entityManager.persist(user);
	}

	public void mergeUser(User user) {
		entityManager.merge(user);
	}

	public User loadUserById(byte[] id) {
		return entityManager.find(User.class, id);
	}

	@SuppressWarnings("unchecked")
	public List<User> findRecentlyCreatedUsers() {
		Query query = entityManager.createQuery("from User where id > ?");
		long threeMinutesAgo = System.currentTimeMillis() - 1000 * 60 * 3;
		query.setParameter(1, new BytesBuilder().add("USER").add(threeMinutesAgo).toBytes());
		return query.getResultList();
	}

	public boolean updateUserNamedTom(byte[] id, String newName) {
		Query query = entityManager.createQuery("update User set name = ? where id = ? and name = ?");
		query.setParameter(1, newName);
		query.setParameter(2, id);
		query.setParameter(3, "Tom");
		return query.executeUpdate() > 0;
	}

	public void deleteUser(User user) {
		entityManager.remove(user);
	}

	public void close() {
		entityManager.clear();
	}
	
	@PersistenceContext
	public void setEntityManager(EntityManager entityManager) {
		this.entityManager = entityManager;
	}

}
