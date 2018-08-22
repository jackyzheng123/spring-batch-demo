package spring.boot.redis.schedule;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import spring.boot.redis.util.DateUtil;

@Component
@EnableScheduling
public class ScheduleTasks {

	@Scheduled(cron = "*/5 * * * * *")
	public void printTime(){
		System.out.println("现在时间是：" + DateUtil.formatDate(DateUtil.YYYY_MM_DD_HH_MM_SS));
	}
}
