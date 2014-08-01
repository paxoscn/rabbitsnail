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

	public void deleteUser(User user) {
		entityManager.remove(user);
	}
	
	@PersistenceContext
	public void setEntityManager(EntityManager entityManager) {
		this.entityManager = entityManager;
	}

}
