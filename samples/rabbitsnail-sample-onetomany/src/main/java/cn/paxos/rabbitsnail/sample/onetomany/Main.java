package cn.paxos.rabbitsnail.sample.onetomany;

import java.math.BigDecimal;
import java.util.UUID;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import cn.paxos.rabbitsnail.sample.onetomany.entity.Order;
import cn.paxos.rabbitsnail.sample.onetomany.entity.OrderItem;
import cn.paxos.rabbitsnail.sample.onetomany.repository.OrderRepository;
import cn.paxos.rabbitsnail.util.BytesBuilder;

public class Main {
	
	public static void main(String[] args) {
		@SuppressWarnings("resource")
		ApplicationContext applicationContext = new ClassPathXmlApplicationContext("/application-context.xml");
		OrderRepository orderRepository = (OrderRepository) applicationContext.getBean("orderRepository");
		
		Order order = new Order();
		order.setCode(UUID.randomUUID().toString());
		byte[] id = new BytesBuilder().add(order.getCode()).toBytes();
		order.setId(id);

		OrderItem item = new OrderItem();
		order.getItems().add(item);
		item.setName("Lakers T-Shirt (Gold/Purple)");
		item.setAmount(new BigDecimal(15.00));

		item = new OrderItem();
		order.getItems().add(item);
		item.setName("iPod Classic 160GB");
		item.setAmount(new BigDecimal(179.00));

		item = new OrderItem();
		order.getItems().add(item);
		item.setName("Air Max 2014 (Blue/Black)");
		item.setAmount(new BigDecimal(89.00));
		
		orderRepository.persistOrder(order);
		
		order = orderRepository.loadOrderById(id);
		System.out.println("Loaded Order: " + order.getCode());
		int itemIndex = 0;
		for (OrderItem item_ : order.getItems()) {
			System.out.println("Item #" + (++itemIndex) + ": " + item_.getName() + " $" + item_.getAmount());
		}
		
		orderRepository.close();
	}

}
