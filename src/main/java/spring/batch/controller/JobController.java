package spring.batch.controller;

import java.io.File;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job importJob;
	
	@Autowired
	private Job exportJob;
	
	public JobParameters jobParameters;
	
	@Value("${file.input.path}")
	private String fileInputPath;
	
	@Value("${file.output.path}")
	private String fileOutputPath;

	/**
	 * 从csv文件导入数据
	 * @return
	 * @throws Exception
	 */
	//定时执行
    @Scheduled(cron = "0 0 10 * * *")
	@RequestMapping("/import")
	public String importData() throws Exception {
		
		if (!new File(fileInputPath).exists() ){
			return "error, 文件不存在";
		}
		jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
				.addString("input.file.name", fileInputPath).toJobParameters();
		jobLauncher.run(importJob, jobParameters);
		return "ok";
	}

	/**
	 * 导出数据到csv文件
	 */
    @Scheduled(cron = "0 05 10 * * *")
	@RequestMapping("/export")
	public String exportDataToCsv() throws Exception{
		
		jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
				.addString("output.file.name", fileOutputPath).toJobParameters();
		jobLauncher.run(exportJob, jobParameters);
		return "ok";
	}
}
