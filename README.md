rabbitsnail
===========

A JPA entity manager for HBase

The RabbitSnail entity manager implements a subset of JPA operations, including persisting/merging/deleting an entity, finding entities by a query, and one-to-many with the wide-table mode.
Only a few JPA annotations are supported. Some new annotations are introduced to adapt HBase like @ColumnFamily.

Tutorial: Hello World

1 Create an entity named 'User' as what you did with other JPA imlementations like Hibernate:

(User.java)

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Version;

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
	@Version
	@ColumnFamily(name = "info")
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}

}

An @Entity is used to make the POJO an persistable entity.
The entity will be mapped to HBase table 'user' which is lowercased from the class name. You can also use an @Table to specify another table name.
An entity need exactly ONE field as the primary key, or the 'rowkey' in HBase, of the mapped table. It is annotated by an @Id and its type must be byte[]. The value of it should be assigned manually before you persist the entity as there is no way to generate the value automatically. Note that once a field is specified as the key field, it will NOT be treated as a column any more and its name will be meaningless.
Each field which is mapped to a column, or a 'qualifier' in HBase, need be annotated by an @ColumnFamily to specify its column family. A field will be mapped to a column with the same name, unless it is annotated by an @Column which can specify another column name.
You can use an @Version on ONE int field at most to tell RabbitSnail to check and increase its value on persisting and merging. For this example, RabbitSnail will call HTableInterface.checkAndPut(row, 'info', 'version', null, put) to persist the entity. On merging, RabbitSnail will first fetch the old version from HBase then call HTableInterface.checkAndPut(row, 'info', 'version', OLD-VERSION, put) to merge it. If the HTableInterface.checkAndPut() returns false, a PersistenceException will be thrown. If there is no version field specified in the entity, RabbitSnail will use HTableInterface.put() for both persisting and merging operations, which means that you can not know whether the data is actually 'inserted' or 'updated' after calling EntityManager.persist() or EntityManager.merge(). Note that once a field is specified as the version field, modifying its value manually by calling the 'setter' method of it won't make sense any more as the value will always be replaced by RabbitSnail automatically.
As you've seen above, all annotations for fields can also be put onto their 'getter' methods.

Here is the mapped table in HBase:

-----------------------------
| Column Family |    Column |
-----------------------------
|          info |      name |
-----------------------------
|          info |   version |
-----------------------------

So prepare it in HBase first:

$ /usr/local/hbase shell

hbase(main)> create 'user' 'info'

2 Persisting the entity:

(UserRepository.java)

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

public class UserRepository {

	private EntityManager entityManager;

	public void persistUser(User user) {
		entityManager.persist(user);
	}
	
	@PersistenceContext
	public void setEntityManager(EntityManager entityManager) {
		this.entityManager = entityManager;
	}

}

(Main.java)

import org.apache.hadoop.hbase.util.Bytes;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Main {
	
	public static void main(String[] args) {
		ApplicationContext applicationContext = new ClassPathXmlApplicationContext("/application-context.xml");
		UserRepository userRepository = (UserRepository) applicationContext.getBean("userRepository");
		
		User user = new User();
		byte[] id = Bytes.toBytes("USER" + System.currentTimeMillis());
		user.setId(id);
		user.setName("Tom");
		userRepository.persistUser(user);
	}

}

Pure JPA style except the manual byte-array id assigning. Right?

3 Needed configurations:

(application-context.xml)

<?xml version="1.0" encoding="UTF-8" ?>
<beans xmlns="http://www.springframework.org/schema/beans"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:tx="http://www.springframework.org/schema/tx"
	xsi:schemaLocation="
                http://www.springframework.org/schema/beans
                http://www.springframework.org/schema/beans/spring-beans.xsd"
	default-autowire="byName">
	<bean class="org.springframework.orm.jpa.support.PersistenceAnnotationBeanPostProcessor" />
	<bean id="entityManagerFactory"
		class="org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean">
		<property name="persistenceUnitName" value="default" />
		<property name="jpaPropertyMap">
			<map>
				<entry key="hbase.zookeeper.quorum" value="192.168.20.200,192.168.20.201,192.168.20.202" />
			</map>
		</property>
	</bean>
	<bean id="userRepository" class="UserRepository" />
</beans>

Change the ZooKeeper quorum configuration to match your environment.

(META-INF/persistence.xml)

<?xml version="1.0" encoding="UTF-8"?>
<persistence xmlns="http://java.sun.com/xml/ns/persistence"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="
      http://java.sun.com/xml/ns/persistence
      http://java.sun.com/xml/ns/persistence/persistence_1_0.xsd"
	version="1.0">
	<persistence-unit name="default">
		<provider>cn.paxos.rabbitsnail.PersistenceProviderImpl</provider>
		<class>User</class>
		<exclude-unlisted-classes>true</exclude-unlisted-classes>
	</persistence-unit>
</persistence>

4 Compile and execute Main.class:

After executing, a new row would be inserted into HBase:

$ /usr/local/hbase shell

hbase(main)> scan 'user'
ROW     COLUMN+CELL
 \x...  column=info:name,    timestamp=14085938..., value=Tom
 \x...  column=info:version, timestamp=14085938..., value=1
 
5 Conclusions:

You can find a complete CRUD example in the 'rabbitsnail/samples/rabbitsnail-sample-helloworld' folder.
In the example, you can see how to use a query, for instance, 'from User where id > ?', to find entities whose keys are in a certain range. Note that 'select' clauses is not supported and only the id field is supported in the 'where' clause.
You can also find how to update fields of an entity by a query, for instance, 'update User set name = ? where id = ? and name = ?'. Note that the 'where' clause must contain a condition which specify the value of the id field because RabbitSnail can only update one row each time. It can also contain ONE additional condition to specify the required value of a field. For this example, RabbitSnail would do a conditional updating by calling HTableInterface.checkAndPut(row, 'info', 'name', required-value, put).
Til now only parameterized queries with '?' are supported.
Calling EntityManager.clear() to close the connection to HBase after finishing your database operations is always recommended.
RabbitSnail also provides a simple tool named BytesBuider to build byte arrays of any size, which may be useful on primary key building.

6 About @OneToMany:

Another example in the 'rabbitsnail/samples/rabbitsnail-sample-onetomany' folder shows how to use a simple one-to-many relationship to associate an entity with its children. Note that there are something to keep in mind:
a) An entity and its children are mapped to one row of a 'wide table';
b) Each child takes a column to store its data. The name of the column is the index of the child.
c) All fields of a child are stored as a plain string formatted like 'FIELD1=VALUE1,FIELD2=VALUE2...' where the values are encoded using UTF-8;
d) The container type of the children must be List<CHILD-TYPE>;
e) The child class must NOT be annotated by @Entity.

e.g. The Order and OrderItem in the example should be mapped in HBase like:

----------------------------------------------------------------------
|           info |                                             items |
----------------------------------------------------------------------
| code | version |                    0 |                    1 | ... |
----------------------------------------------------------------------
|  001 |       1 | name=Apple,amount=10 |   name=Pear,amount=3 | ... |
----------------------------------------------------------------------
|  002 |       1 |   name=Bike,amount=2 |                  ... | ... |
----------------------------------------------------------------------



RabbitSnail is still under development so if you find any bugs or need any absent features please let us know.
Thank you.

Mergen Wu <unrealwalker#126.com> (Replace # with @)