package cn.paxos.rabbitsnail.sample.helloworld;

import java.util.List;

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
		byte[] id1 = new BytesBuilder().add("USER").add(System.currentTimeMillis()).toBytes();
		user.setId(id1);
		user.setName("Tom");
		userRepository.persistUser(user);
		
		user = new User();
		byte[] id2 = new BytesBuilder().add("USER").add(System.currentTimeMillis()).toBytes();
		user.setId(id2);
		user.setName("Jerry");
		userRepository.persistUser(user);
		
		user = userRepository.loadUserById(id2);
		System.out.println("Loaded User: " + user.getName());

		boolean updated = userRepository.updateUserNamedTom(id1, "Felix");
		// Successful.
		System.out.println("Updated Tom's name successfully? " + updated);
		updated = userRepository.updateUserNamedTom(id1, "Carfield");
		// Failed as Tom has renamed to Felix.
		System.out.println("Failed to update Tom's name? " + !updated);
		
		user.setName("Mickey");
		userRepository.mergeUser(user);
		
		List<User> users = userRepository.findRecentlyCreatedUsers();
		// Felix and Mickey expected.
		for (User user_ : users) {
			System.out.println("Found User: " + user_.getName());
			userRepository.deleteUser(user_);
		}
		
		userRepository.close();
	}

}
