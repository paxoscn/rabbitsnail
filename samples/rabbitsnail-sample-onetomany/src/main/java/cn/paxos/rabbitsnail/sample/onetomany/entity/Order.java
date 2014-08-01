package cn.paxos.rabbitsnail.sample.onetomany.entity;

import java.util.LinkedList;
import java.util.List;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Version;

import cn.paxos.rabbitsnail.ColumnFamily;

@Entity
public class Order {

	private byte[] id;
	private String code;
	private int version;
	private List<OrderItem> items = new LinkedList<>();
	
	@Id
	public byte[] getId() {
		return id;
	}
	public void setId(byte[] id) {
		this.id = id;
	}
	@ColumnFamily(name = "info")
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	@Version
	@ColumnFamily(name = "info")
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	@OneToMany
	@ColumnFamily(name = "items")
	public List<OrderItem> getItems() {
		return items;
	}
	public void setItems(List<OrderItem> items) {
		this.items = items;
	}

}
