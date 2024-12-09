package com.assignment.controller;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestJson {

	public static void main(String[] args) {
	        String json = "{ " +
	                "\"name\":\"arvind\"," +
	                "\"address\":\"hamirpur\"," +
	                "\"dateOfBirth\":\"20/12/1985\"" +
	                "}";
	        ObjectMapper objectMapper = new ObjectMapper();
	        try {
	            Person person = objectMapper.readValue(json, Person.class);
	            System.out.println("Person = " + person.toString());
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }

}
