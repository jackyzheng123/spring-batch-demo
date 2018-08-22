package spring.batch.job;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.validator.Validator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import spring.batch.entity.User;
import spring.batch.listener.CsvJobListener;
import spring.batch.processor.CsvItemProcessor;
import spring.batch.validator.CsvBeanValidator;

@Configuration
public class CsvToDbJob {

	@Bean
	@StepScope
	public FlatFileItemReader<User> importReader(@Value("#{jobParameters['input.file.name']}") String pathToFile)
			throws Exception {
		FlatFileItemReader<User> reader = new FlatFileItemReader<User>(); //
		reader.setLinesToSkip(1);
		reader.setResource(new FileSystemResource(pathToFile)); //
		reader.setEncoding("UTF-8");
		reader.setLineMapper(new DefaultLineMapper<User>() {
			{ //
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "name", "age", "sex" });
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<User>() {
					{
						setTargetType(User.class);
					}
				});
			}
		});

		return reader;
	}

	@Bean
	@StepScope
	public ItemProcessor<User, User> importProcessor() {
		CsvItemProcessor processor = new CsvItemProcessor();
		processor.setValidator(csvBeanValidator());
		return processor;
	}

	@Bean
	@StepScope
	public ItemWriter<User> importWriter(DataSource dataSource) {
		JdbcBatchItemWriter<User> writer = new JdbcBatchItemWriter<User>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<User>());
		String sql = "insert into t_user(name,age,sex) values(:name, :age, :sex)";
		writer.setSql(sql); // 3
		writer.setDataSource(dataSource);
		return writer;
	}

	@Bean
	public Job importJob(JobBuilderFactory jobs, Step importStep) {
		return jobs.get("importJob").incrementer(new RunIdIncrementer()).flow(importStep).end().listener(csvJobListener())
				.build();
	}

	@Bean
	@Qualifier("importStep")
	public Step importStep(StepBuilderFactory stepBuilderFactory, ItemReader<User> importReader, ItemWriter<User> importWriter,
			ItemProcessor<User, User> importProcessor) {
		return stepBuilderFactory.get("importStep")
				.<User, User> chunk(5000)
				.reader(importReader)
				.processor(importProcessor)
				.writer(importWriter).build();
	}

	@Bean
	public CsvJobListener csvJobListener() {
		return new CsvJobListener();
	}

	@Bean
	public Validator<User> csvBeanValidator() {
		return new CsvBeanValidator<User>();
	}
}
