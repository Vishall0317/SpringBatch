package com.springbatch.config;

import org.springframework.batch.item.ItemProcessor;

import com.springbatch.entity.Employee;

public class EmployeeItemProcessor implements ItemProcessor<Employee, Employee>{

	@Override
	public Employee process(Employee employee) throws Exception {
		return employee;
	}

}
