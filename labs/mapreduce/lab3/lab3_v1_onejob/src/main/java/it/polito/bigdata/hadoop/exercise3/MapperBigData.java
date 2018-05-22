package it.polito.bigdata.hadoop.exercise3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab 3v1 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            //split record
            String[] splittedRecord = value.toString().split(",");

            //create collection of products, ignoring the userId
            ArrayList<String> products = new ArrayList<>();
            for(int i=1; i<splittedRecord.length; i++)
                products.add(splittedRecord[i]);

            /*
            * sort the collection in alphabetical order, so that the product occurs always in the same order for every mapper
            * this is required in order to avoid pair with the same meaning, such as: productA_productB , productB_productA
            */
            Collections.sort(products);

            //produce all the productA_productB pairs
            ArrayList<String> productsPairs = new ArrayList<>();

            for(int i=0; i<products.size()-1; i++){ //avoid last product

                String productA = products.get(i);

                for(int j=i+1; j<products.size(); j++){

                    String productB = products.get(j);

                    productsPairs.add(productA + "_" + productB);
                }
            }

            //emit all the couples
            for(int i=0; i< productsPairs.size(); i++)
                context.write(new Text(productsPairs.get(i)), new IntWritable(1));

    }
}
