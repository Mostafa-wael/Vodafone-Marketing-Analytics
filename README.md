# Marketing Analytics

## Statment of the problem
Vodafone Egypt is launching a marketing campaign in Ramadan to promote their sales and increase their profit from selling the prepaid recharge cards. These cards are worth 5, 10, 15, 50, and 100 EGP. 

The data science team at Vodafone are analyzing the customers’ data which include the customer personal information, the prepaid card they purchased, the timestamp they registered the prepaid amount on their Vodafone accounts, among other information. 

The details of the customers are omitted, and you are only provided with a file “in.csv” which includes two columns. 
Customer ID. (Each ID maps to a certain customer, whose data is hidden for confidentiality). 
Prepaid Card Amount. 

YWe want to generate a report using MapReduce (similar to the WordCount program) showing the total amount of prepaid cards for each customer that they have purchased. For example, if a customer with ID 300 purchased 5 cards with 10, 15, 15, 10, 100, then the report should include that customer ID 300 bought cards with a total amount of 150. 
>> Disclaimer: Thanks to Vodafone DS team who provided us with this real customer data. 


## Implementation
### Map Function
```java
  // Tha mapper function will read a csv file with an id and a card price and will emit the card price for each id
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text(); // To store the word

    // This is the map function
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(","); // Tokenize the input
      word.set(tokens[0]); // Set the word
      context.write(word, new IntWritable(Integer.parseInt(tokens[1]))); // Emit the word and the price
    }
  }
```
### Reduce Function
``` java
  // The reduccer is the same as the wordcount example
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable(); // To store the sum of the words

    // This is the reduce function
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0; // To store the sum of the words
      // For each word, add the count
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum); // Set the result
      context.write(key, result); // Emit the word and the sum
    }
  }
```
### Driver
```java
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); // Create a new configuration
    Job job = Job.getInstance(conf, "count cards"); // Create a new job
    job.setJarByClass(CountCards.class); // Set the jar by class
    job.setMapperClass(TokenizerMapper.class); // Set the mapper class
    job.setCombinerClass(IntSumReducer.class); // Set the combiner class
    job.setReducerClass(IntSumReducer.class); // Set the reducer class
    job.setOutputKeyClass(Text.class); // Set the output key class
    job.setOutputValueClass(IntWritable.class); // Set the output value class
    FileInputFormat.addInputPath(job, new Path(args[0])); // Set the input path
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // Set the output path
    System.exit(job.waitForCompletion(true) ? 0 : 1); // Wait for the job to complete
  }
```
>> You can find the full code in the file CountCards.java

## Running the code
>> The code ran on a docker container with hadoop installed on it. You can find the docker containers [here](https://github.com/big-data-europe/docker-hadoop)
1. Open a terminal and run the following commands:
    ```bash
    git clone https://github.com/big-data-europe/docker-hadoop
    docker-compose up -d
    ``` 
2. Open another terminal in the code directory and run the following commands:
    ``` bash
    docker cp ./CountCards.java namenode:/tmp/
    docker cp ./in.csv namenode:/tmp/
    docker exec -it namenode /bin/bash
    cd /tmp/
    mkdir countcards
    mv CountCards.java ./countcards/
    mv in.csv ./countcards/
    cd countcards/
    mkdir classes
    ls
    export HADOOP_CLASSPATH=$(hadoop classpath)
    hadoop fs -mkdir /countcards
    hadoop fs -mkdir /countcards/Input
    hadoop fs -put /tmp/countcards/in.csv /countcards/Input
    javac -classpath $HADOOP_CLASSPATH -d ./classes/ ./CountCards.java 
    jar -cvf CountCards.jar -C ./classes .
    hadoop jar CountCards.jar CountCards  /countcards/Input/in.csv /countcards/Output
    hadoop fs -cat /countcards/Output/*
    hadoop fs -cat /countcards/Output/* > output.txt
    cat output.txt 
    ```
3. Return to first terminal and run the following commands:
    ```bash
    docker cp namenode:/tmp/countcards/output.csv output.csv
    docker-compose down
    ```