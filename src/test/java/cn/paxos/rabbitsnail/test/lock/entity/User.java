package cn.paxos.rabbitsnail.test.lock.entity;

import javax.persistence.Entity;
import javax.persistence.Id;

import cn.paxos.rabbitsnail.ColumnFamily;

@Entity
public class User {

	private byte[] id;
	private String name;
	private int version;
	
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
	@ColumnFamily(name = "info")
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}

}
