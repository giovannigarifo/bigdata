package it.polito.bigdata.hadoop.lab4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.net.URI;
import java.util.HashMap;

import com.sun.jndi.toolkit.url.Uri;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        FloatWritable> {// Output value type

    HashMap<String,Float> usrAvgScoreMap; //contains for each user his average score

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {

        usrAvgScoreMap = new HashMap<>();

        String line;
        Path[] PathsCachedFiles = context.getLocalCacheFiles();
        BufferedReader file = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));

        // Iterate over the lines of the file
        while ((line = file.readLine()) != null) {

            String[] splittedLine = line.split("\\t+");
            usrAvgScoreMap.put(splittedLine[0], new Float(splittedLine[1]));
        }
        file.close();
    }

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] splittedReview = value.toString().split(",");

        //if not header of the csv emit the values
        if (!splittedReview[0].equals("Id")) {

            //score & user
            String userID = splittedReview[2];
            Integer score = new Integer(splittedReview[6]);

            //normalize score
            Float normalizedScore = score - usrAvgScoreMap.get(userID);

            //emit (PID,NormalizedScore)
            context.write(new Text(splittedReview[1]), new FloatWritable(normalizedScore));
        }
    }
}
