package spring.boot.redis.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import spring.boot.redis.config.MyException;

/**
 * 统一异常测试
 * @author zhengjiaxing
 * @date 2018年8月20日
 */
@RestController
public class HelloController {

	@RequestMapping("/hello")
	public String hello() throws Exception{
//		throw new Exception("异常！！！");
		throw new MyException("我的异常！");
	}
}
