package cn.paxos.rabbitsnail.sample.helloworld;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import cn.paxos.rabbitsnail.sample.helloworld.entity.User;
import cn.paxos.rabbitsnail.sample.helloworld.repository.UserRepository;
import cn.paxos.rabbitsnail.util.BytesBuilder;

public class Main {
	
	public static void main(String[] args) {
		@SuppressWarnings("resource")
		ApplicationContext applicationContext = new ClassPathXmlApplicationContext("/application-context.xml");
		UserRepository userRepository = (UserRepository) applicationContext.getBean("userRepository");
		User user = new User();
		byte[] id = new BytesBuilder().add("USER").add(System.currentTimeMillis()).toBytes();
		user.setId(id);
		user.setName("Tom");
		userRepository.persistUser(user);
		user = userRepository.loadUserById(id);
		System.out.println("Loaded User: " + user.getName() + " (version = " + user.getVersion() + ")");
		user.setName("Jerry");
		userRepository.mergeUser(user);
		user = userRepository.loadUserById(id);
		System.out.println("Loaded User: " + user.getName() + " (version = " + user.getVersion() + ")");
	}

}
