package org.zhanggw.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

public class SubmitScalaJobToYarn {

	public static void main(String[] args) {
		System.setProperty("SPARK_YARN_MODE", "true");  
		System.setProperty("HADOOP_USER_NAME", "root");	//change submit user

		String[] arg0 = new String[]{ 
			"--jar", "/D:/Documents/workspace-idea/SparkStreaming/SparkStreamingTest/target/sparkStreaming-1.0-SNAPSHOT-jar-with-dependencies.jar",
	        "--class", "org.tairan.zhanggw.StreamingTest", 
//	        "--num-executors", "3",		//invalid, use spark.executor.instances in SparkConf
	        "--executor-cores", "6",
	        "--executor-memory", "2G",
	        "--driver-cores", "8",
	        "--driver-memory", "4G",
	        "--name", "spark-streaming"
		};  

		Configuration conf = new Configuration();
		String os = System.getProperty("os.name");
		boolean cross_platform = false;
		if(os.contains("Windows") || os.contains("windows")) {
			cross_platform = true;
		}
		conf.setBoolean("mapreduce.app-submission.cross-platform", cross_platform);
		
		conf.set("fs.defaultFS", "hdfs://hadoop.nd4.trcloud.com.novalocal:8020/");
		conf.set("mapreduce.framework.name","yarn"); 
		conf.set("yarn.resourcemanager.hostname", "hadoop.nd4.trcloud.com.novalocal");  
		conf.set("yarn.resourcemanager.scheduler.address","hadoop.nd4.trcloud.com.novalocal:8030");
		conf.set("yarn.resourcemanager.resource-tracker.address","hadoop.nd4.trcloud.com.novalocal:8031");
		conf.set("yarn.resourcemanager.address","hadoop.nd4.trcloud.com.novalocal:8032");
		
		SparkConf sparkConf = new SparkConf();
		
		/***************very important**************/
		sparkConf.set("spark.yarn.dist.files", "C:\\Users\\hzzgw\\Desktop\\etc\\yarn-site.xml");
		sparkConf.set("spark.executor.instances", "3");
		/***************very important**************/
		
		ClientArguments arg = new ClientArguments(arg0, sparkConf);
		Client client = new Client(arg, conf, sparkConf);
		//client.run();
		System.out.println(client.submitApplication());
	}

}
