package com.springbatch.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.springbatch.entity.Employee;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	private DataSource dataSource;
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public FlatFileItemReader<Employee> reader(){
		
		FlatFileItemReader<Employee> reader = new FlatFileItemReader<>();
		
		reader.setResource(new ClassPathResource("records.csv"));
		reader.setLineMapper(getLineMapper());
		reader.setLinesToSkip(1);
		
		return reader;
	}

	private LineMapper<Employee> getLineMapper() {
		
		DefaultLineMapper<Employee> lineMapper = new DefaultLineMapper<>();
		
		//extracts rows and columns
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames(new String[] {"id", "name", "mobile"});
		lineTokenizer.setIncludedFields(new int[] {0, 1, 9});
		lineMapper.setLineTokenizer(lineTokenizer);
		
		//wrap above rows and columns with entity
		BeanWrapperFieldSetMapper<Employee> fieldSetMapper=new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(Employee.class);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		
		return lineMapper;
	}
	
	@Bean
	public EmployeeItemProcessor processor() {
		return new EmployeeItemProcessor();
	}
	
	@Bean
	public JdbcBatchItemWriter<Employee> writer() {
		JdbcBatchItemWriter<Employee> writer = new JdbcBatchItemWriter<>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Employee>());
		writer.setSql("insert into employee(id, name, mobile) values (:id, :name, :mobile)");
		writer.setDataSource(this.dataSource);
		return writer;
	}
	
	@Bean
	public Job importUserJob() {
		return this.jobBuilderFactory.get("EMPLOYEE-IMPORT-JOB")
				.incrementer(new RunIdIncrementer())
				.flow(step1())
				.end() 
				.build();
	}

	@Bean
	public Step step1() {
		return this.stepBuilderFactory.get("step1")
		.<Employee, Employee>chunk(10)
		.reader(reader())
		.processor(processor())
		.writer(writer())
		.build();
	}	
}
