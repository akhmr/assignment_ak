package com.assignment.controller;

import java.io.Serializable;

public class Person implements Serializable{
	
	private String name;
	
	private String address;
	
	private String dateOfBirth;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getDateOfBirth() {
		return dateOfBirth;
	}

	public void setDateOfBirth(String dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}

	@Override
	public String toString() {
		return "Person [name=" + name + ", address=" + address + ", dateOfBirth=" + dateOfBirth + "]";
	}

}
