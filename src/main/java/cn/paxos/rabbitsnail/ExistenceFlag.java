package cn.paxos.rabbitsnail;

import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import cn.paxos.rabbitsnail.matcher.ColumnValueMatcher;

@Retention(RetentionPolicy.RUNTIME)
@Target(value={TYPE})
public @interface ExistenceFlag {

	String family();
	String column();
	Class<? extends ColumnValueMatcher> matcher();

}
