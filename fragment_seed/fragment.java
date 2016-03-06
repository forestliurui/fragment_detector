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
			
			Set<String> parents_seed1_set = new HashSet<String>();
			Set<String> children_seed1_set = new HashSet<String>();
			Set<String> parents_seed2_set = new HashSet<String>();
                        Set<String> children_seed2_set = new HashSet<String>();


			if(nodeAncestors.containsKey(seed1)){
				StringTokenizer parents_seed1_tokens=new StringTokenizer(nodeAncestors.get(seed1),",");
				while(parents_seed1_tokens.hasMoreTokens()){
					parents_seed1_set.add(parents_seed1_tokens.nextToken());
				}
			}
			if(nodeDecedants.containsKey(seed1)){
                                StringTokenizer children_seed1_tokens=new StringTokenizer(nodeDecedants.get(seed1),",");
                                while(children_seed1_tokens.hasMoreTokens()){ 
                                        children_seed1_set.add(children_seed1_tokens.nextToken());
                                }
                        }
			if(nodeAncestors.containsKey(seed2)){
                                StringTokenizer parents_seed2_tokens=new StringTokenizer(nodeAncestors.get(seed2),",");
                                while(parents_seed2_tokens.hasMoreTokens()){ 
                                        parents_seed2_set.add(parents_seed2_tokens.nextToken());
                                }
                        }
                        if(nodeDecedants.containsKey(seed2)){
                                StringTokenizer children_seed2_tokens=new StringTokenizer(nodeDecedants.get(seed2),",");
                                while(children_seed2_tokens.hasMoreTokens()){ 
                                        children_seed2_set.add(children_seed2_tokens.nextToken());
                                }
                        }
			

			
			Set<String> common_parents_set =  new HashSet<String>();
			Set<String> common_children_set =  new HashSet<String>();

			common_parents_set.addAll(parents_seed1_set);
			common_parents_set.retainAll(parents_seed2_set);
			common_children_set.addAll(children_seed1_set);
                        common_children_set.retainAll(children_seed2_set);

			ArrayList<String> upb = new ArrayList<String>();// upb is the string list for the upper bound of two seeds
			ArrayList<String> lowb = new ArrayList<String>();//lowb is the string list for the lower bound of two seeds
			for (String x1: common_parents_set){
				upb.add(x1);
			}
			for (String x1: common_children_set){
                                lowb.add(x1);
                        }
/*
			//token non-lattice pairs lowbounds
			StringTokenizer keytokenizer = new StringTokenizer(keypair,"/");
			ArrayList<String> lowb = new ArrayList<String>();
			while (keytokenizer.hasMoreTokens()) {
				lowb.add(keytokenizer.nextToken());
			}
			
			//token non-lattice pairs upbounds
			StringTokenizer lbtokenizer = new StringTokenizer(lowbounds,",");
			ArrayList<String> upb = new ArrayList<String>();
			while (lbtokenizer.hasMoreTokens()) {
				upb.add(lbtokenizer.nextToken());
			}
*/


			Set<String> Sset = new HashSet<String>();
			Set<String> upSet = new HashSet<String>();
			Set<String> downSet = new HashSet<String>();
//			up set
			for(String low:lowb) {
				if (nodeAncestors.containsKey(low)) {
					
					StringTokenizer tempAncestorsTokens = new StringTokenizer(nodeAncestors.get(low),",");
					while(tempAncestorsTokens.hasMoreTokens()){
						upSet.add(tempAncestorsTokens.nextToken());
					}
				}
				
			}
//			down set.

			for(String up:upb) {
				if (nodeDecedants.containsKey(up)) {
					StringTokenizer tempDecedantsTokens = new StringTokenizer(nodeDecedants.get(up),",");
					while(tempDecedantsTokens.hasMoreTokens()){
						downSet.add(tempDecedantsTokens.nextToken());
					}
				}
			}
//			intersect
			
			Sset.addAll(upSet);
			Sset.retainAll(downSet);
/*			
			System.out.print("Print out upSet:\n");
			for(String x1:upSet){
				System.out.print(x1);
			}			
			System.out.print("Print out upSet: finished\n");

			System.out.print("Print out downSet:\n");
                        for(String x1:downSet){
                                System.out.print(x1);
                        }                       
                        System.out.print("Print out downSet: finished\n");
*/
//			Build output
			StringBuilder stringBuilder = new StringBuilder();
			for(String lowkey:lowb){
				stringBuilder.append(",");
				stringBuilder.append(lowkey);
			}
			for(String upkey:upb){
				stringBuilder.append(",");
				stringBuilder.append(upkey);
			}
			for(String x:Sset) {
				stringBuilder.append(",");
				stringBuilder.append(x);
			}
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
			context.write(key, new Text (nodesBuilder.substring(1)+"\t"+edgesBuilder.substring(1)));
		}
	}


}
