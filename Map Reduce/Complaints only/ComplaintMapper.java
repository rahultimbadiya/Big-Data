import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class ComplaintMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text firstCharacter = new Text();

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

  
      String[] line = value.toString().split(",");
  

      try{
	  if(true)
	      {
		  String val1=line[1];
		  context.write(new Text(val1),new IntWritable(1));
		  
	      }

      
	
         }
    catch(Exception e)
	{
	}
  }
}
