package cn.paxos.rabbitsnail.sample.helloworld.entity;

import javax.persistence.Entity;
import javax.persistence.Id;

import cn.paxos.rabbitsnail.ColumnFamily;
import cn.paxos.rabbitsnail.ExistenceFlag;
import cn.paxos.rabbitsnail.matcher.NullMatcher;

@Entity
@ExistenceFlag(family = "info", column = "name", matcher = NullMatcher.class)
public class User {

	private byte[] id;
	private String name;
	
	@Id
	public byte[] getId() {
		return id;
	}
	public void setId(byte[] id) {
		this.id = id;
	}
	@ColumnFamily(name = "info")
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

}
