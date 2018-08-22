package spring.boot.redis;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.annotation.Transactional;

import junit.framework.Assert;
import spring.boot.redis.base.BaseJunit4Test;
import spring.boot.redis.entity.User;
import spring.boot.redis.util.RedisUtil;

@SuppressWarnings("deprecation")
public class RedisTest extends BaseJunit4Test{

	@Autowired
	private StringRedisTemplate stringRedisTemplate;
	
	@Autowired
	private RedisTemplate<Object, Object> redisTemplate;
	
	@Autowired
	private RedisUtil redisUtil;
	
	@Test
	@Transactional
	public void strRedisTemplateTest(){
		stringRedisTemplate.opsForValue().set("aaa", "111");
		Assert.assertEquals("111", stringRedisTemplate.opsForValue().get("aaa"));
	}
	
	@Test
	public void objRedisTemplateTest(){
		User user = new User();
		user.setName("zjx");
		user.setAge(18);
		redisTemplate.opsForValue().set("zjx", user);
		redisTemplate.expire("zjx", 20, TimeUnit.SECONDS);
		Assert.assertEquals(18, ((User) redisTemplate.opsForValue().get("zjx")).getAge());
	}
	
	/**
	 * 字符型写入、获取缓存
	 */
	@Test
	public void redisStrTest(){
		Assert.assertEquals(true, redisUtil.set("name", "jacky"));
		Assert.assertEquals("jacky", redisUtil.get("name"));
	}

	/**
	 * 字符型写入、获取缓存,设置有效时间
	 */
	@Test
	public void redisStrTimeTest(){
		Assert.assertEquals(true, redisUtil.set("age", "18", 20));
		Assert.assertEquals("18", redisUtil.get("age"));
	}
	
	/**
	 * 移除
	 */
	@Test
	public void redisStrRemoveTest(){
		System.out.println(redisUtil.get("aaa"));
		redisUtil.remove("aaa");
		System.out.println(redisUtil.get("aaa"));
	}
	
	/**
	 * 批量删除
	 */
	@Test
	public void redisStrRemoveBatchTest(){
		redisUtil.remove("aaa","bbb","ccc");
		Assert.assertEquals(null, redisUtil.get("aaa"));
		Assert.assertEquals(null, redisUtil.get("bbb"));
		Assert.assertEquals(null, redisUtil.get("ccc"));
	}
	
	/**
	 * 匹配删
	 */
	@Test
	public void redisStrRemovePatternTest(){
		redisUtil.removePattern("a*");
	}
	
	/**
	 * 是否存在
	 */
	@Test
	public void reidsExistTest(){
		Assert.assertEquals(true, redisUtil.exists("aaa"));
	}
	
	/**
	 * 哈希添加,获取
	 */
	@Test
	public void addHmSetTest(){
		redisUtil.hmSet("a", "a1", "a11");
		redisUtil.hmSet("a", "a2", "a22");
		redisUtil.hmSet("a", "a3", "a33");
		Assert.assertEquals("a33", redisUtil.hmGet("a", "a3"));
	}
	
	/**
	 * 列表添加获取
	 */
	@Test
	public void listAddTest(){
		redisUtil.lPush("str", "String");
		redisUtil.lPush("str", "StringBuffer");
		redisUtil.lPush("str", "StringBuilder");
		
		List<Object> list = redisUtil.lRange("str", 1, 3);
		for (Object obj : list) {
			System.out.println(obj.toString());
		}
	}
	
	/**
	 * 集合添加获取
	 */
	@Test
	public void setTest(){
		User user1 = new User("zzz", 18);
		User user2 = new User("yyy", 20);
		redisUtil.add("user", user1);
		redisUtil.add("user", user2);
		
		Set<Object> set = redisUtil.setMembers("user");
		for (Object obj : set) {
			User user = (User) obj;
			System.out.println(user.toString());
		}
	}
	
	/**
	 * 有序集合添加获取
	 */
	@Test
	public void zSetTest(){
		redisUtil.zAdd("name", "Jacky", 5);
		redisUtil.zAdd("name", "Max", 2);
		redisUtil.zAdd("name", "CC", 1);
		redisUtil.zAdd("name", "TT", 3);
		redisUtil.zAdd("name", "PP", 4);
		
		Set<Object> set = redisUtil.rangeByScore("name", 3, 5);
		for (Object obj : set) {
			System.out.println(obj.toString());
		}
	}
}
