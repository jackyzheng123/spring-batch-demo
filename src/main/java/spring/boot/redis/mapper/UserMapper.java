package spring.boot.redis.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;

import spring.boot.redis.entity.User;

//@Mapper
//@CacheConfig(cacheNames = "users")
public interface UserMapper {

	//@Insert("insert into t_user(name, age) values(#{name}, #{age})")
	//@CachePut(key="#p.name")
	int addUser(User user);
	
	//@Delete("delete from t_user where name=#{name}")
	int deleteUser(User user);
	
	//@Update("update t_user set name=#{name}, age=#{age} where name=#{name}")
	int updateUser(User user);
	
	//@Select("select * from t_user where name = #{name}")
	//@Cacheable(key = "#p")
	User getUser(@Param("name") String name);
	
	//@Select("select * from t_user limit #{pageNum}, #{pageSize}")
	List<User> queryUserList(@Param("pageNum") int pageNum, @Param("pageSize") int pageSize);
	
	
}
