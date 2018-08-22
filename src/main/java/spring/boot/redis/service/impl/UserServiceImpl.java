package spring.boot.redis.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import spring.boot.redis.common.Constant;
import spring.boot.redis.entity.BaseDto;
import spring.boot.redis.entity.PageData;
import spring.boot.redis.entity.PageDto;
import spring.boot.redis.entity.User;
import spring.boot.redis.mapper.UserMapper;
import spring.boot.redis.service.UserService;

@Service
public class UserServiceImpl implements UserService {

	@Autowired
	private UserMapper userMapper;
	
	@Override
	public BaseDto<User> getUser(String name) {
		BaseDto<User> dto = new BaseDto<User>();
		
		User user = userMapper.getUser(name);
		if (null == user) {
			dto.setRestCode(Constant.ERROR_CODE);
			dto.setRestMsg("用户不存在");
			return dto;
		}
		dto.setRestCode(Constant.SUCCESS_CODE);
		dto.setRestMsg("查询用户信息成功");
		dto.setData(user);
		return dto;
	}

	@Override
	public PageDto<List<User>> queryUserList(int pageNum) {
		PageDto<List<User>> dto = new PageDto<List<User>>();
		if (pageNum <= 0) {
			dto.setRestCode(Constant.ERROR_CODE);
			dto.setRestMsg("页数不正确");
			return dto;
		}
		
		pageNum = (pageNum - 1) * Constant.PAGE_SIZE;
		List<User> list = userMapper.queryUserList(pageNum, Constant.PAGE_SIZE);
		if (null == list) {
			dto.setRestCode(Constant.ERROR_CODE);
			dto.setRestMsg("查询结果为空");
			return dto;
		}
		PageHelper.startPage(pageNum, Constant.PAGE_SIZE);
		PageInfo<User> pageInfo = new PageInfo<User>(list);
		PageData pageData = new PageData(pageInfo.getTotal(), pageInfo.getPages(), pageInfo.getPageSize());
		
		dto.setRestCode(Constant.SUCCESS_CODE);
		dto.setRestMsg("查询用户列表成功");
		dto.setPageData(pageData);
		dto.setData(list);
		
		return dto;
	}

	@Override
	@Transactional
	public BaseDto<String> addUser(User user) {
		BaseDto<String> dto = new BaseDto<String>();
		int row = userMapper.addUser(user);
		if (row > 0 ) {
			dto.setRestCode(Constant.SUCCESS_CODE);
			dto.setRestMsg("添加用户成功");
		} else {
			dto.setRestCode(Constant.ERROR_CODE);
			dto.setRestMsg("添加用户失败");
		}
		return dto;
	}

}
