package it.polito.bigdata.hadoop.exercise2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Path inputPath;
    Path outputDir;
    String filter_strPrefix; //the filter used to select only a subset of the words, the ones which begin with strPrefix
	int exitCode;  
	
	// Parse the parameters, no reducerers: map only job
    if(args.length != 3){

        System.out.println("\n[ERROR] wrong parameter number. use: <input-path> <output-dir> filter_strPrefix");
        System.exit(1);
    }


    inputPath = new Path(args[0]);
    outputDir = new Path(args[1]);
    filter_strPrefix = new String(args[2]);
    
    Configuration conf = this.getConf();

    //set a configuration parameter
    conf.set("strPrefix", filter_strPrefix);

    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Lab #2 - filter");
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath);
    
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    // Set job input format
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    //conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
       
    // Set map class
    job.setMapperClass(MapperBigData.class);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    // Set reduce class
    //job.setReducerClass(ReducerBigData.class);
        
    // Set reduce output key and value classes
    //job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(IntWritable.class);

    // Set number of reducers
    job.setNumReduceTasks(0);
    
    
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true)
    	exitCode=0;
    else
    	exitCode=1;
    	
    return exitCode;
  }
  

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

    System.exit(res);
  }
}