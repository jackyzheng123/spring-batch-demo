package spring.boot.redis.service;

import java.util.List;

import spring.boot.redis.entity.BaseDto;
import spring.boot.redis.entity.PageDto;
import spring.boot.redis.entity.User;

public interface UserService {

	BaseDto<User> getUser(String name);

	PageDto<List<User>> queryUserList(int pageNum);

	BaseDto<String> addUser(User user);
}
