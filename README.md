
# Sping-kafka-beam-demo

Step  to run the code on the local

Requirement: 
                 1) Install kafka <br/>
                 2)  Java 17 minimum <br/>
                 

Step 1: Create kafka topic mentioned in application.properties <br/>
Step 2: make sure kafka up and running <br/>
Step 3: Change the log path in logback-spring.xml based on your os. <br/>
        current path given is <property name="LOGS" value="var/logs/assignment" />.You can change it <br/>
Step 4: Run the spring boot services . <br/>

Note: Port configured for service is 8001 <br/>


Sample Curl :  curl --location 'http://localhost:8001/send' \
--header 'Content-Type: application/json' \
--data '{ 
   "name":"arvind",
	"address":"gurgaon",
	"dateOfBirth":"20/12/1986"
}'

if server is up and running you can use sample curl. <br/>

How application work: <br/>
We will push data through rest api which is given above , than same data is being pushed to Kafka <br/>
From Kafka data is cosumned by apache beam pipeline and age is calculated , <br/>
after age is odd or even is being calculated and  data is being pushed either in odd or even topic. <br/>
Data is being logged from these topic. <br/>
