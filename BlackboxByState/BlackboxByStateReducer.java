// reducer function for application to compute join of
// ADU file with file containing application names
// for system ports (port numbers < 1024).  The key for
// the join operation is the port number.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class BlackboxByStateReducer
  extends Reducer<Text, IntWritable, Text, LongWritable>{

    //lists holding entries with the same key
    // private ArrayList<Text> S_list = new ArrayList<Text>(); //State list
    // private ArrayList<Text> R_list = new ArrayList<Text>(); //Rtt list


  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {

    long count = 0;
      // iterate through all the values (count == 1) with a common key
      for (IntWritable value : values) {
          count = count + value.get();
      }
    context.write(key, new LongWritable(count));
       
  }
}
