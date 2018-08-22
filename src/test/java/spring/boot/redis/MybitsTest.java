package spring.boot.redis;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import junit.framework.Assert;
import spring.boot.redis.base.BaseJunit4Test;
import spring.boot.redis.entity.User;
import spring.boot.redis.mapper.UserMapper;

@SuppressWarnings("deprecation")
public class MybitsTest extends BaseJunit4Test{
	
	@Autowired
	private UserMapper userMapper;

	@Test
	@Rollback
	@Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRED)
	public void test(){
		Assert.assertEquals(1, userMapper.addUser(new User("eee", 5)));;
	}
	
	@Test
	public void cacheTest(){
		System.out.println("first:" + userMapper.getUser("zjx").toString());
		System.out.println("second:" + userMapper.getUser("zjx").toString());
	}
}
