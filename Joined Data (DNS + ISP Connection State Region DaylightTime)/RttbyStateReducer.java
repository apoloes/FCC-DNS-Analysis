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

public class RttbyStateReducer
  extends Reducer<Text, Text, NullWritable, Text> {

    //lists holding entries with the same key
    private ArrayList<Text> S_list = new ArrayList<Text>(); //State list
    private ArrayList<Text> R_list = new ArrayList<Text>(); //Rtt list


  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

      // clear the lists
      S_list.clear();
      R_list.clear();

      // iterate through all the values with a common key (Port)
      for (Text value : values) {
	      if (value.charAt(0) == 'S') { //from State file, get the state
          S_list.add(new Text(value.toString().substring(1)));
	      } else if (value.charAt(0) == 'R') {// from Rtt file, get the Rtt
          R_list.add(new Text(value.toString().substring(1)));
        }
      }

       // If both lists are not empty, join Rtt data and State name 
      if (!S_list.isEmpty() && !R_list.isEmpty()) {
        for (Text S : S_list) {
	        for (Text R : R_list) {
		        context.write(NullWritable.get(), new Text(R.toString() + "," + S.toString()));
          }
        }
      }
       
  }
}
