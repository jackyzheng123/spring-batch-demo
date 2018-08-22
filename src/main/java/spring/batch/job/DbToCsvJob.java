package spring.batch.job;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

import spring.batch.entity.User;
import spring.batch.processor.DbToCsvItemProcessor;
import spring.batch.rowmapper.UserRowMapper;

@Configuration
public class DbToCsvJob {
	
	@Bean
	@StepScope
	public ItemReader<User> exportReader(DataSource dataSource) throws UnexpectedInputException, ParseException, Exception {
		JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<User>();
		reader.setDataSource(dataSource);
		reader.setSql("select * from t_user where age > ?");
		reader.setRowMapper(userRowMapper());
		reader.setPreparedStatementSetter(new PreparedStatementSetter() {
			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				ps.setInt(1, 18);
			}
		});
		ExecutionContext executionContext = new ExecutionContext();
		reader.open(executionContext);
		return reader;
	}

	@Bean
	public RowMapper<User> userRowMapper() {
		return new UserRowMapper();
	}

	@Bean
	@StepScope
	public ItemProcessor<User, User> exportProcessor() {
		return new DbToCsvItemProcessor();
	}

	@Bean
	@StepScope
	public FlatFileItemWriter<User> exportWriter(@Value("#{jobParameters['output.file.name']}") String pathToFile)
			throws Exception {
		FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>(); //
		writer.setResource(new FileSystemResource(pathToFile)); //
		writer.setEncoding("UTF-8");
		// 表头处理 TODO
		DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<User>();
		lineAggregator.setDelimiter(",");
		BeanWrapperFieldExtractor<User> extractor = new BeanWrapperFieldExtractor<User>();
		extractor.setNames(new String[] { "name", "age", "sex" });
		lineAggregator.setFieldExtractor(extractor);
		writer.setLineAggregator(lineAggregator);
		return writer;
	}

	@Bean
	public Job exportJob(JobBuilderFactory jobs, Step exportStep) {
		return jobs.get("exportJob")
				.incrementer(new RunIdIncrementer())
				.flow(exportStep)
				.end()
				.build();
	}

	@Bean
	@Qualifier("exportStep")
	public Step exportStep(StepBuilderFactory stepBuilderFactory, ItemReader<User> exportReader, ItemWriter<User> exportWriter,
			ItemProcessor<User, User> exportProcessor) {
		return stepBuilderFactory.get("exportStep")
				.<User, User> chunk(5000)
				.reader(exportReader)
				.processor(exportProcessor)
				.writer(exportWriter).build();
	}

}
