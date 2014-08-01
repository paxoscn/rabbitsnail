package cn.paxos.rabbitsnail.sample.onetomany.repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import cn.paxos.rabbitsnail.sample.onetomany.entity.Order;

public class OrderRepository {

	private EntityManager entityManager;

	public void persistOrder(Order order) {
		entityManager.persist(order);
	}

	public Order loadOrderById(byte[] id) {
		return entityManager.find(Order.class, id);
	}
	
	@PersistenceContext
	public void setEntityManager(EntityManager entityManager) {
		this.entityManager = entityManager;
	}

}
