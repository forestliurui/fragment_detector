//package edu.cwru.cs.snomedct.Fragment;
//package Fragment;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.ImmutableBiMap.Builder;

import java.util.ArrayList;


@SuppressWarnings("deprecation")
public class fragment {
	
	public static class fragMapper extends Mapper<Object, Text, Text, Text>{
		private Text result= new Text();
	    private Text middle = new Text();
		HashMap<String, String> nodeAncestors = new HashMap<String, String>();
		HashMap<String, String> nodeDecedants = new HashMap<String, String>();
		Path[] localPaths = new Path[0];
		
		protected void setup(Context context) throws IOException, InterruptedException {

			try {
				localPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	          } catch (IOException ioe) {
	            System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
	          }	
			for (Path p:localPaths) {
			BufferedReader joinReader = new BufferedReader(new FileReader(p.getName()));
			String cacheline = new String();
			        try {
			              while ((cacheline = joinReader.readLine()) != null) {
			            	  if(cacheline.contains("@")) {
			            		  StringTokenizer tokenizer = new StringTokenizer(cacheline,"@");
					              String first = tokenizer.nextToken();
							      String second = tokenizer.nextToken();
							      nodeDecedants.put(first, second);
			            	  } else if (cacheline.contains("#")){
			            		  StringTokenizer tokenizer = new StringTokenizer(cacheline,"#");
			            		  String first = tokenizer.nextToken();
							      String second = tokenizer.nextToken();
							      nodeAncestors.put(first, second);
			            	  }
			             
			                }
			              } finally {
			                   joinReader.close();
			                }
			}
			System.out.println(nodeDecedants.size());
			System.out.println(nodeAncestors.size());
			}
	
//		@SuppressWarnings("deprecation")

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
/*			
			//token non-lattice results
			StringTokenizer tokenizer = new StringTokenizer(line,"\t");
			String keypair = tokenizer.nextToken();
			String lowbounds = tokenizer.nextToken();
*/
			//keypair is the string of low bound delimited by /
			//lowbounds is the string of upper bound delimited by ,

			StringTokenizer tokenizer = new StringTokenizer(line, "\t");
			String seed1 = tokenizer.nextToken();
			String seed2 = tokenizer.nextToken();

			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(",");
			stringBuilder.append(seed1);
			stringBuilder.append(",");
			stringBuilder.append(seed2);
	
//			System.out.print(keypair);
//			System.out.print(stringBuilder);
			result.set(line);
			middle.set(stringBuilder.substring(1));
			
			context.write(result,middle);
		}

	}


	public static class fragReducer extends Reducer<Text, Text, Text, Text>{
		HashSet<String> originalEdges = new HashSet<String>();
		Path[] localPaths = new Path[0];
		
		protected void setup(Context context) throws IOException, InterruptedException {

			try {
				localPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	          } catch (IOException ioe) {
	            System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
	          }	
			for (Path p:localPaths) {
			BufferedReader joinReader = new BufferedReader(new FileReader(p.getName()));
			String cacheline = new String();
			        try {
			              while ((cacheline = joinReader.readLine()) != null) {
			            	  if(cacheline.contains("|")) {
//			            		  StringTokenizer tokenizer = new StringTokenizer(cacheline,"@");
//					              String first = tokenizer.nextToken();
//							      String second = tokenizer.nextToken();
							      originalEdges.add(cacheline);
			            	  }
			             
			                }
			              } finally {
			                   joinReader.close();
			                }
			}
			System.out.println("reduce");

			}

		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {

			Set<String> edgeSet = new HashSet<String>();
			ArrayList<String> nodes = new ArrayList<String>();
			for (Text t:values) {
				StringTokenizer tokenizer = new StringTokenizer(t.toString(), ",");
				while(tokenizer.hasMoreTokens()) {
					nodes.add(tokenizer.nextToken());
				}
//				context.write(key, t);
			}
				

			for (int i = 0; i < nodes.size();i++) {
				for (int j = 0; j < nodes.size(); j++) {
					if (originalEdges.contains(nodes.get(i)+"|"+nodes.get(j))){
						edgeSet.add(nodes.get(i)+"|"+nodes.get(j));
					}
				}
			}
			StringBuilder nodesBuilder = new StringBuilder();
			for (String x:nodes) {
				nodesBuilder.append(",");
				nodesBuilder.append(x);
			}
			StringBuilder edgesBuilder = new StringBuilder();
			for (String x:edgeSet) {
				edgesBuilder.append(",");
				edgesBuilder.append(x);
			}
			try{
				context.write(key, new Text (nodesBuilder.substring(1)+"\t"+edgesBuilder.substring(1)));
			}catch(StringIndexOutOfBoundsException e){
				context.write(key, new Text ("\t"));
				
			}
		}

	}


}
