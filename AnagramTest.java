import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Anagram {
	
	public static class AnagramTestMapper extends
		Mapper<Object, Text, Text, Text> {   //LongWritable rather than Object?
			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {
				StringTokenizer itr = new StringTokenizer(value.toString());
				while (itr.hasMoreTokens()) {
					String word = itr.nextToken().replaceAll("[^a-zA-Z0-9]", "");
					char[] arr = word.toCharArray();
					Arrays.sort(arr);
					String wordKey = new String(arr);
					context.write(new Text(wordKey), new Text(word));
				}
			}
		}
		
	public static class AnagramTestReducer
		extends Reducer<Text, Text, Text, Text> {
			private Text testword = new Text();
			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
				HashSet<String> anagram = new HashSet<>();
				for (Text val : values) {
					anagram.add(val.toString());
				}
				ArrayList<String> list = new ArrayList<String>(anagram);
				
				try {
					URL url = new URL("https://www.textfixer.com/tutorials/common-english-words-with-contractions.txt");
					
					Scanner sc = new Scanner(url.openStream());
					
					while (sc.hasNextLine()) {
						String currentLine = sc.nextLine();
						String [] words = currentLine.split(",");
						for(String a : words) {
							if(list.contains(a)) {
								list.remove(a);
							}
						}
					}
				}
				catch (IOException e) {
					e.printStackTrace();
				}
					
				Collections.sort(list);
				testword.set(list.toString());
				
				if(list.size() > 1) {
					context.write(key, testword);
				}
			}
		}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "anagram");
        job.setJarByClass(Anagram.class);
        job.setMapperClass(AnagramTestMapper.class);
        job.setReducerClass(AnagramTestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
