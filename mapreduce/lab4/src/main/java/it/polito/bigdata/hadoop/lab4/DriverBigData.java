package it.polito.bigdata.hadoop.lab4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import it.polito.bigdata.hadoop.lab4.DriverBigData;
import it.polito.bigdata.hadoop.lab4.MapperBigData1;
import it.polito.bigdata.hadoop.lab4.MapperBigData2;
import it.polito.bigdata.hadoop.lab4.ReducerBigData1;
import it.polito.bigdata.hadoop.lab4.ReducerBigData2;

import java.net.URI;

/**
 * MapReduce program
 *
 * to launch on cluster:
 *
 * hadoop jar main.jar it.polito.bigdata.hadoop.lab4.DriverBigData 1 lab4_v1/input lab4_v1/output_job1 lab4_v1/output_job2
 */
public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		int exitCode;

		Configuration conf = this.getConf();

		// Define a new job
		Job job = Job.getInstance(conf);

		// Assign a name to the job
		job.setJobName("Lab#4 - Ex.1 - step 1");

		/*
		 * ********************************************************* 
		 * Fill out the missing parts/update the content of this method
		 ************************************************************
		*/
		Path inputPath;
		Path outputDirJob1;
		int numberOfReducersJob1;

		// Parse the parameters for the set up of the first job
		numberOfReducersJob1 = Integer.parseInt(args[0]);
		inputPath = new Path(args[1]);
        outputDirJob1 = new Path(args[2]);

		// Set the path of the input file/folder for this first job
		FileInputFormat.addInputPath(job, inputPath);

		// Set the path of the output folder for this job
		FileOutputFormat.setOutputPath(job, outputDirJob1);

		// Specify the class of the Driver for this job
		job.setJarByClass(DriverBigData.class);

		// Set job input format
		job.setInputFormatClass(TextInputFormat.class);

		// Set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set map class
		job.setMapperClass(MapperBigData1.class);

		// Set map output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Set reduce class
		job.setReducerClass(ReducerBigData1.class);

		// Set reduce output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		// Set number of reducers
		job.setNumReduceTasks(numberOfReducersJob1);

		// Execute the first job and wait for completion
		if (job.waitForCompletion(true) == true) {
			// Set up the second job
			Job job2 = Job.getInstance(conf);

			// Assign a name to the second job
			job2.setJobName("Lab#3 - Ex.1 - step 2");

			// Add the output of job1 as cached file
            job2.addCacheFile(new URI(outputDirJob1.toString() + "/part-r-00000"));

			/* */
			// Change the following part of the code
			Path outputDir2;
			int numberOfReducersJob2;
			outputDir2 = new Path(args[3]);

			// Set path of the input file/folder for this second job
			// The output of the first job is the input of this second job    
			FileInputFormat.addInputPath(job2, inputPath);

			// Set path of the output folder for this job
			FileOutputFormat.setOutputPath(job2, outputDir2);

			// Specify the class of the Driver for this job
			job2.setJarByClass(DriverBigData.class);

			// Set job input format
			job2.setInputFormatClass(TextInputFormat.class);

			// Set job output format
			job2.setOutputFormatClass(TextOutputFormat.class);

			// Set map class
			job2.setMapperClass(MapperBigData2.class);

			// Set map output key and value classes
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(FloatWritable.class);

			// Set reduce class
			job2.setReducerClass(ReducerBigData2.class);

			// Set reduce output key and value classes
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(FloatWritable.class);

			// Set the number of reducers of the second job equal to number of reducers of first job
			numberOfReducersJob2 = numberOfReducersJob1;
			job2.setNumReduceTasks(numberOfReducersJob2);

			// Execute the second job and wait for completion
			if (job2.waitForCompletion(true) == true)
				exitCode = 0;
			else
				exitCode = 1;
		} else
			exitCode = 1;

		return exitCode;

	}

	/**
	 * Main of the driver
	 */

	public static void main(String args[]) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop
		// application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

		System.exit(res);
	}
}
