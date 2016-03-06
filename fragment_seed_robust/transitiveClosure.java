//package edu.cwru.cs.snomed.transitiveClosure;
//package transitiveClosure;

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

import java.util.ArrayList;

@SuppressWarnings("deprecation")
public class transitiveClosure {
	
	public static class tcMapper extends Mapper<Object, Text, Text, Text>{
		private Text node= new Text();
	    private Text word = new Text();
		HashMap<String, String> ancestors = new HashMap<String, String>();

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
			            	  if(cacheline.contains("\t")) {
			            		  StringTokenizer tokenizer = new StringTokenizer(cacheline,"\t");
					              String first = tokenizer.nextToken().toString();
							      String second = tokenizer.nextToken().toString();
								      ancestors.put(first, second);
							     							      
			            	  } 
			             
			                }
			              } finally {
			                   joinReader.close();
			                }
			}
			System.out.println(ancestors.size());
			System.out.println("xx");

			}

//		@SuppressWarnings("deprecation")

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
//			StringTokenizer pairToken = new StringTokenizer(line,"\t");
//			String pair = pairToken.nextToken();
//			String lowbound = pairToken.nextToken();
//			StringTokenizer nodeToken = new StringTokenizer(pair,"/");
//			String firstnode = nodeToken.nextToken();
//			String secondnode = nodeToken.nextToken();
//			
//			StringTokenizer lbToken = new StringTokenizer(lowbound,",");
//			HashSet<String> lbFma = new HashSet<String>();
//			while (lbToken.hasMoreTokens()){
//				String lbTemp = lbToken.nextToken();
//				if(ancestors.containsKey(lbTemp)) {
//				String fmaTemp = ancestors.get(lbTemp);
//				StringTokenizer fmaToken = new StringTokenizer(fmaTemp,",");
//				while (fmaToken.hasMoreTokens()){
//					lbFma.add(fmaToken.nextToken());
//				}
//				}
//			}
//			String fmaMatchLb = new String ();
//			StringBuilder builder = new StringBuilder();
//			if (lbFma.isEmpty()) {
//				fmaMatchLb = "No Match Lowbounds";
//			} else {
//				for(String t : lbFma) {
//					builder.append(t.toString()).append(",");
//					}
//				builder.setLength(builder.length() - 1);
//				fmaMatchLb = builder.toString();
//			}
//			if (ancestors.containsKey(firstnode) && ancestors.containsKey(secondnode) ){
//				String firstFma = ancestors.get(firstnode);
//				String secondFma = ancestors.get(secondnode);
//				StringTokenizer fFToken = new StringTokenizer(firstFma,",");
//				StringTokenizer sFToken = new StringTokenizer(secondFma,",");
//				ArrayList<String> ff = new ArrayList<String>();
//				ArrayList<String> sf = new ArrayList<String>();
//
//				while(fFToken.hasMoreTokens()){
//					ff.add(fFToken.nextToken());
//				}
//				while(sFToken.hasMoreTokens()){
//					sf.add(sFToken.nextToken());
//				}
//				String fmaPair = new String();
//				for(String x:ff){
//					for(String y:sf){
//						fmaPair = fmaPair + "#" + x + "/" + y;
//					}
//				}
//				if(fmaMatchLb != "No Match Lowbounds") {
//				context.write(new Text(pair), new Text(fmaPair+"@@@"+fmaMatchLb));
//				}
//			}
			
			
			node.set(new Text(line));
			if (ancestors.containsKey(line)){
			String firstlevel = ancestors.get(line);
			StringTokenizer tokenizer = new StringTokenizer(firstlevel,",");
			ArrayList<String> levelContainer = new ArrayList<String>();
			while(tokenizer.hasMoreTokens()) {
				levelContainer.add(tokenizer.nextToken());
			}

			HashSet<String> Ance = new HashSet<String>();
			for(String x:levelContainer) {
				Ance.add(x);
			}
//			System.out.println("xx");
			while(levelContainer.size()>0){
				ArrayList<String> tempContainer = new ArrayList<String>();
				for(String x:levelContainer) {
					if (ancestors.containsKey(x)){
						String tempnode = ancestors.get(x);
						StringTokenizer tempTokenizer = new StringTokenizer(tempnode,",");
//						System.out.println(tempContainer.toString());
						while(tempTokenizer.hasMoreTokens()) {
							String nodeToDecide = tempTokenizer.nextToken();
							if (!Ance.contains(nodeToDecide)){
								tempContainer.add(nodeToDecide);
							}
						}
					}
					
				}

				if (tempContainer.size()>0) {
					levelContainer = tempContainer;
					for(String x:tempContainer) {
						Ance.add(x);
					}
				} else {
					levelContainer.clear();
				}
				
			}

			for (String x:Ance) {
				context.write(node, new Text(x));
			}
			} else {
				context.write(node, new Text("No ancestor"));
			}
//			
			
		}

	}


	public static class tcReducer extends Reducer<Text, Text, Text, Text>{

		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			for(Text x:values) {
				if (!x.toString().contains("No ancestor")) {
					context.write(key, new Text(x.toString()));
				}
			}
			
		}
	}


}
