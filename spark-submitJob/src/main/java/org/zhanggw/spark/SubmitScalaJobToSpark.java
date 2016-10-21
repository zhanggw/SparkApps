package org.zhanggw.spark;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.deploy.SparkSubmit;

public class SubmitScalaJobToSpark {

	public static void main(String[] args) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
		String filename = dateFormat.format(new Date());
		String tmp=Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String jarFile = tmp.substring(0, tmp.length()-4) + "lib/paradise-core-1.0-jar-with-dependencies.jar";
		String curFile = "/E:/work/krebons/paradise/paradise-core/target/paradise-core-1.0-jar-with-dependencies.jar";
		
		String[] arg0 = new String[] {
			"--master", "spark://hadoop008:7077",
			"--name", "SparkSubmit" + "_" + filename,
			curFile,
			"test.txt"
		};
		
		SparkSubmit.main(arg0);
	}

}
