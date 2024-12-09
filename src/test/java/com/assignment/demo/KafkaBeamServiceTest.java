package com.assignment.demo;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.assignment.service.KafkaBeamService.EvenLengthFilterFn;
import com.assignment.service.KafkaBeamService.OddLengthFilterFn;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest
public class KafkaBeamServiceTest {
	
	private EvenLengthFilterFn evenLengthFilterFn;
	private OddLengthFilterFn oddLengthFilterFn;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        evenLengthFilterFn = new EvenLengthFilterFn();
        oddLengthFilterFn = new OddLengthFilterFn();
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testEvenAgeProcessing() throws Exception {
        String validMessage = "{ \"dateOfBirth\": \"01/01/1990\" }"; 
        OutputReceiver<String> mockOutputReceiver = mock(OutputReceiver.class);
        evenLengthFilterFn.processElement(validMessage, mockOutputReceiver);
        verify(mockOutputReceiver).output("Age is even and age is =34"); 
    }
    @Test
    public void testOddAgeProcessing() throws Exception {
        String validMessage = "{ \"dateOfBirth\": \"01/01/1989\" }"; 
        OutputReceiver<String> mockOutputReceiver = mock(OutputReceiver.class);
        oddLengthFilterFn.processElement(validMessage, mockOutputReceiver);
        verify(mockOutputReceiver).output("Age is odd and age is =35"); 
    }




}