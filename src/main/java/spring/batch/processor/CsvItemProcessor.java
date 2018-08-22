package spring.batch.processor;

import javax.validation.ValidationException;

import org.springframework.batch.item.validator.ValidatingItemProcessor;

import spring.batch.entity.User;

public class CsvItemProcessor extends ValidatingItemProcessor<User> {

	@Override
	public User process(User item) throws ValidationException {
		super.process(item); // 需要执行super.process(item)才会调用自定义校验器

		if (item.getSex().equalsIgnoreCase("F")) {
			item.setSex("0");
		} else {
			item.setSex("1");
		}
		return item;
	}

}