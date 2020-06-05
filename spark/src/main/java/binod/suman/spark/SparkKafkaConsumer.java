package binod.suman.spark;



import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class SparkKafkaConsumer {

	public static void main(String[] args) throws IOException {
		
		System.out.println("Spark Streaming started now .....");

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// batchDuration - The time interval at which streaming data will be divided into batches
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("demo");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		  List<String> allRecord = new ArrayList<String>();
		  final String COMMA = ",";
		  
		  directKafkaStream.foreachRDD(rdd -> {
			  
		  System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
			  if(rdd.count() > 0) {
				rdd.collect().forEach(rawRecord -> {
					  
					  System.out.println(rawRecord);
					  System.out.println("***************************************");
					  System.out.println(rawRecord._2);
					  String record = rawRecord._2();
					  StringTokenizer st = new StringTokenizer(record,",");
					  
					  StringBuilder sb = new StringBuilder(); 
					  while(st.hasMoreTokens()) {
						String step = st.nextToken(); // Maps a unit of time in the real world. In this case 1 step is 1 hour of time. 
						String type = st.nextToken(); // CASH-IN,CASH-OUT, DEBIT, PAYMENT and TRANSFER 
						String amount = st.nextToken(); //amount of the transaction in local currency 
						String nameOrig = st.nextToken(); //  customerID who started the transaction 
						String oldbalanceOrg = st.nextToken(); // initial balance before the transaction 
						String newbalanceOrig = st.nextToken(); // customer's balance after the transaction.
						String nameDest = st.nextToken(); // recipient ID of the transaction. 
						String oldbalanceDest = st.nextToken(); // initial recipient balance before the transaction. 
						String newbalanceDest = st.nextToken(); // recipient's balance after the transaction. 
						String isFraud = st.nextToken(); // dentifies a fraudulent transaction (1) and non fraudulent (0) 
						String isFlaggedFraud = st.nextToken(); // flags illegal attempts to transfer more than 200.000 in a single transaction.
					    // Keep only interested columnn in Master Data set.
						sb.append(step).append(COMMA).append(type).append(COMMA).append(amount).append(COMMA).append(oldbalanceOrg).append(COMMA).append(newbalanceOrig).append(COMMA).append(oldbalanceDest).append(COMMA).append(newbalanceDest).append(COMMA).append(isFraud);
						allRecord.add(sb.toString());
					  }
					  
				  });
				System.out.println("All records OUTER MOST :"+allRecord.size()); 
				FileWriter writer = new FileWriter("Master_dataset.csv");
				for(String s : allRecord) {
					writer.write(s);
					writer.write("\n");
				}
				System.out.println("Master dataset has been created : ");
				writer.close();
			  }
		  });
		 
		ssc.start();
		ssc.awaitTermination();
	}
	

}



