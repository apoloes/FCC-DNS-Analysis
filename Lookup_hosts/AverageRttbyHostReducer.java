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

public class AverageRttbyHostReducer
  extends Reducer<Text, Text, Text, Text>{

    //lists holding entries with the same key
    // private ArrayList<Text> S_list = new ArrayList<Text>(); //State list
    // private ArrayList<Text> R_list = new ArrayList<Text>(); //Rtt list


  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

    long count = 0;
    float rtt_sum = 0.0f;

    // rtt_list.clear();

    for (Text value : values) {
      count++;
      rtt_sum += Float.parseFloat(value.toString());
      // rtt_list.add(Float.parseFloat(value.toString()));
    }

    //calculate the mean
    float mean = rtt_sum / count;

    context.write(key, new Text(Float.toString(mean)));
       
  }
}
