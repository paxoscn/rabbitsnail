package cn.paxos.rabbitsnail.sample.helloworld.repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import cn.paxos.rabbitsnail.sample.helloworld.entity.User;

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
	
	@PersistenceContext
	public void setEntityManager(EntityManager entityManager) {
		this.entityManager = entityManager;
	}

}
