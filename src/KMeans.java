/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 *
 *
 * @author Himank Chaudhary
 */


import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Reducer;

@SuppressWarnings("deprecation")
public class KMeans {
 
    public static String OUT = "outfile";
    public static String IN = "inputlarger";
    public static List<Double> centers = new ArrayList<Double>();
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {
        
        @Override
        public void configure(JobConf job) {
            System.out.println("inside configure()");
            try {
				// Fetch the file from Distributed Cache Read it and store the centroid in the ArrayList
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    System.out.println("Inside setup(): "+ cacheFiles[0].toString());
                    String line;
                    centers.clear();
                    BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    try {
                        while ((line = cacheReader.readLine()) != null) {
                            String[] temp = line.split("\t| ");
                            centers.add(Double.parseDouble(temp[0]));
                        }
                    } finally {
                        cacheReader.close();
                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistribtuedCache: " + e);
            }
        }
        @Override
        public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, DoubleWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            double point = Double.parseDouble(line);
            double min1, min2 = Double.MAX_VALUE, nearest_center = centers.get(0);
                // Find the minimum center from a point
				for (double c : centers) {
                	min1 = c - point;
                	if (Math.abs(min1) < Math.abs(min2)) {
                        nearest_center = c;
                        min2 = min1;
                    }
                    
                }
            // Emit the nearest center and the point
            output.collect(new DoubleWritable(nearest_center), new DoubleWritable(point));
            
        }
    }
 
    public static class Reduce extends MapReduceBase implements Reducer<DoubleWritable, DoubleWritable, DoubleWritable, Text> { 
        @Override
        public void reduce(DoubleWritable key, Iterator<DoubleWritable> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
            double newCenter;
            double sum = 0;
            int no_elements = 0;
			String points = "";
            while (values.hasNext()) {
				double d = values.next().get();
				points = points + " " + Double.toString(d);
                sum = sum + d;
                ++no_elements;
            }
            // Find new center
			newCenter = sum/no_elements;
            // Emit new center and point
			output.collect(new DoubleWritable(newCenter), new Text(points));
    }
  } 
   public static void main(String[] args) throws Exception {
        run(args);
    }
    public static void run(String[] args) throws Exception {
        IN = args[0];
        OUT = args[1];
        String input = IN;
        String output = OUT + System.nanoTime();
        String again_input = output;
        boolean isdone = false;
        int iteration = 0;
		// Reiterating till the convergence
        while (isdone == false) {
            JobConf conf = new JobConf(KMeans.class);
            if (iteration == 0) {
                Path hdfsPath = new Path(input + "/centroid.txt");
                // upload the file to hdfs. Overwrite any existing copy.
                 DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
            } else {
                Path hdfsPath = new Path(again_input + "/part-00000");
                // upload the file to hdfs. Overwrite any existing copy.
                 DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			                          
            }
            conf.setJobName("KMeans");
            conf.setMapOutputKeyClass(DoubleWritable.class);
            conf.setMapOutputValueClass(DoubleWritable.class);
            conf.setOutputKeyClass(DoubleWritable.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(conf, new Path(input + "/data.txt"));
            FileOutputFormat.setOutputPath(conf, new Path(output));
            JobClient.runJob(conf);
            Path ofile = new Path(output + "/part-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
            List<Double> centers_next = new ArrayList<Double>();
            String line = br.readLine();
            while (line != null){
                String[] sp = line.split("\t| ");
                double c = Double.parseDouble(sp[0]);
                centers_next.add(c);
                line=br.readLine();
            }
            br.close();
			String prev;
			if (iteration == 0)
				prev = input + "/centroid.txt";
			else
				prev = again_input + "/part-00000";
			Path prevfile = new Path(prev);
            FileSystem fs1 = FileSystem.get(new Configuration());
            BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(prevfile)));
            List<Double> centers_prev = new ArrayList<Double>();
            String l = br1.readLine();
            while (l != null){
                String[] sp1 = l.split("\t| ");
                double d = Double.parseDouble(sp1[0]);
                centers_prev.add(d);
                l=br1.readLine();
            }
            br1.close();
            //Sort the old centroid and new centroid and check for convergence condition
			Collections.sort(centers_next);
			Collections.sort(centers_prev);
			Iterator<Double> it = centers_prev.iterator();
			for (double d: centers_next) {
                double temp = it.next();  
				if (Math.abs(temp-d) <= 0.1) {
                    isdone = true;
                } else {
                    isdone = false;
                    break;
                }
                 //}
			}
             
			++iteration;
			again_input = output;
			output = OUT + System.nanoTime();
       }
      
    }

}