package spring.boot.redis.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import spring.boot.redis.common.Constant;
import spring.boot.redis.entity.BaseDto;
import spring.boot.redis.entity.PageDto;
import spring.boot.redis.entity.User;
import spring.boot.redis.service.UserService;

@RestController
@RequestMapping("/user")
public class UserController {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private UserService userService;

	/**
	 * 查询用户
	 * 
	 * @param name
	 * @return
	 */
	@RequestMapping("/user-info")
	public BaseDto<User> getUser(@RequestParam("name") String name) {
		BaseDto<User> dto = null;
		try {
			dto = userService.getUser(name);
		} catch (Exception e) {
			dto = new BaseDto<User>();
			dto.setRestCode(Constant.ERROR_CODE);
			dto.setRestMsg("查询用户异常，" + e.getMessage());
			logger.error("查询用户异常，" + e.getMessage(), e);
		}
		return dto;
	}

	/**
	 * 查询用户列表
	 * 
	 * @param pageNum
	 *            当前页
	 * @return
	 */
	@RequestMapping("/user-list")
	public PageDto<List<User>> queryUserList(@RequestParam("pageNum") int pageNum) {
		PageDto<List<User>> dto = null;
		try {
			dto = userService.queryUserList(pageNum);
		} catch (Exception e) {
			dto = new PageDto<List<User>>();
			dto.setRestCode(Constant.ERROR_CODE);
			dto.setRestMsg("查询用户列表异常，" + e.getMessage());
			logger.error("查询用户列表异常，" + e.getMessage(), e);
		}

		return dto;
	}

	/**
	 * 添加用户
	 */
	@RequestMapping(value = "/addUser", method = RequestMethod.POST)
	public BaseDto<String> addUser(@RequestBody User user) {
		BaseDto<String> dto = null;
		try {
			dto = userService.addUser(user);
		} catch (Exception e) {
			dto = new BaseDto<String>();
			dto.setRestCode(Constant.ERROR_CODE);
			dto.setRestMsg("添加用户异常，" + e.getMessage());
			logger.error("添加用户异常，" + e.getMessage(), e);
		}
		return dto;
	}
}
