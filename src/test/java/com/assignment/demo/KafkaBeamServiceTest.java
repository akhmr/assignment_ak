package com.assignment.demo;

import java.util.Collections;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.assignment.service.KafkaBeamService.ProcessedMessageFn;

@RunWith(JUnit4.class)
public class KafkaBeamServiceTest {

	@Rule
    public final TestPipeline pipeline = TestPipeline.create();
   // Pipeline pipeline = TestPipeline.create();

    @BeforeClass
    public static void setUp() {
        PipelineOptionsFactory.register(TestPipelineOptions.class);
    }

    @Test
    public void testValidEvenAgeMessage() {
        String validJson = "{\"name\":\"John\", \"dateOfBirth\":\"01/01/2000\"}";
        PCollection<KV<String, String>> output = pipeline
            .apply("Create", Create.of(validJson))
            .apply("Process", ParDo.of(new ProcessedMessageFn()));

        PAssert.that(output)
               .containsInAnyOrder(
                   KV.of("odd", "Age is odd and age is =25")
               );

        pipeline.run();
    }
    
    @Test
    public void testValidOddAgeMessage() {
        String validJson = "{\"name\":\"John\", \"dateOfBirth\":\"01/01/2001\"}";
        PCollection<KV<String, String>> output = pipeline
            .apply("Create", Create.of(validJson))
            .apply("Process", ParDo.of(new ProcessedMessageFn()));

        PAssert.that(output)
               .containsInAnyOrder(
                   KV.of("even", "Age is even and age is =24")
               );

        pipeline.run();
    }
    
    @Test
    public void testInvalidJson() {
        String invalidJson = "invalid json string";

        PCollection<KV<String, String>> output = pipeline
            .apply("Create", Create.of(invalidJson))
            .apply("Process", ParDo.of(new ProcessedMessageFn()));

        PAssert.that(output)
               .containsInAnyOrder(
                   KV.of("error", "exception occurred")
               );

        pipeline.run();
    }
}