package spring.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import spring.batch.entity.User;

public class DbToCsvItemProcessor implements ItemProcessor<User, User> {

	@Override
	public User process(User item) throws Exception {

		if (null != item && item.getSex().equals("0")) {
			item.setSex("女");
		} else {
			item.setSex("男");
		}
		return item;
	}

}
