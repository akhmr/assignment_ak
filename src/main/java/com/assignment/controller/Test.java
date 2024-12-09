package com.assignment.controller;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Test {

    public static void main(String[] args) {

        // Step 1: Create the Pipeline.
        Pipeline pipeline = Pipeline.create();

//        // Step 2: Define input data as a PCollection.
//        PCollection<Integer> celsiusTemps = pipeline.apply(Create.of(0, 25, 100));
//        
//      //  PCollection<Integer> pc = pipeline.apply(Create.of(3, 4, 5).withCoder(BigEndianIntegerCoder.of()));
//        
//       // Create.of(0, 25, 100) 
//
//        // Step 3: Apply a Map transform to convert Celsius to Fahrenheit.
//        PCollection<Integer> fahrenheitTemps = celsiusTemps.apply(
//            MapElements.into(TypeDescriptors.integers()) // Define the output type as Integer.
//                       .via(temp -> (int) (temp * 9.0 / 5.0 + 32)) // Transformation logic.
//        );
//
//        // Step 4: Output the converted temperatures using ParDo.
//        fahrenheitTemps.apply(ParDo.of(new DoFn<Integer, Void>() {
//            @ProcessElement
//            public void processElement(@Element Integer temp) {
//                // Print each temperature to the console.
//                System.out.println("Temperature in Fahrenheit: " + temp);
//            }
//        }));
        
        PCollection<String> reviewsFromSourceA = pipeline.apply("SourceAReviews", Create.of("Excellent", "Very good"));
        PCollection<String> reviewsFromSourceB = pipeline.apply("SourceBReviews", Create.of("Average", "Good"));

        PCollection<String> allReviews = PCollectionList.of(reviewsFromSourceA)
                .and(reviewsFromSourceB)
                .apply(Flatten.pCollections());

        allReviews.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(@Element String review) {
                System.out.println(review);
            }
        }));

        pipeline.run().waitUntilFinish();

        // Step 5: Execute the pipeline.
       // pipeline.run().waitUntilFinish();
    }
}
