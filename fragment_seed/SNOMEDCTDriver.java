//package edu.cwru.cs.snomedct;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import edu.cwru.cs.snomed.fma.fma;
//import edu.cwru.cs.snomed.fma.fma.fmaMapper;
//import edu.cwru.cs.snomed.fma.fma.fmaReducer;
//import transitiveClosure.*;
//import transitiveClosure.tcMapper;
//import transitiveClosure.tcReducer;
//import edu.cwru.cs.snomed.transitiveClosure.transitiveClosure;
//import edu.cwru.cs.snomed.transitiveClosure.transitiveClosure.tcMapper;
//import edu.cwru.cs.snomed.transitiveClosure.transitiveClosure.tcReducer;

//import edu.cwru.cs.snomedct.minimalCA.minimalCA;
/*import edu.cwru.cs.snomedct.minimalCA.minimalCA.minimalMapper;
import edu.cwru.cs.snomedct.minimalCA.minimalCA.minimalReducer;
import edu.cwru.cs.snomedct.CATjoin.CATjoin;
import edu.cwru.cs.snomedct.CATjoin.CATjoin.CATMapper;
import edu.cwru.cs.snomedct.CATjoin.CATjoin.CATReducer;
import edu.cwru.cs.snomedct.cartesianproduct.CarPro;
import edu.cwru.cs.snomedct.cartesianproduct.CarPro.CPMapper;
import edu.cwru.cs.snomedct.cartesianproduct.CarPro.CPReducer;
import edu.cwru.cs.snomedct.commonAncestor.CommonAncestor;
import edu.cwru.cs.snomedct.commonAncestor.CommonAncestor.CAMapper;
import edu.cwru.cs.snomedct.commonAncestor.CommonAncestor.CAReducer;

import edu.cwru.cs.snomedct.mapreducers.ancestorStep.TCtoAncestorMapper;
import edu.cwru.cs.snomedct.mapreducers.ancestorStep.TCtoAncestorReducer;
*/
//import Fragment.*;
//import Fragment.fragment.fragMapper;
//import Fragment.fragment.fragReducer;
//import edu.cwru.cs.snomedct.Fragment.*;
//import edu.cwru.cs.snomedct.Fragment.fragment.fragMapper;
//import edu.cwru.cs.snomedct.Fragment.fragment.fragReducer;

/*
import edu.cwru.cs.snomedct.LowBond.*;
import edu.cwru.cs.snomedct.LowBond.LowBond.LBMapper;
import edu.cwru.cs.snomedct.LowBond.LowBond.LBReducer;
*/

@SuppressWarnings("deprecation")
public class SNOMEDCTDriver extends Configured implements Tool {

	private static final boolean FIND_UPPER_BOUND_FROM_TC = false;
	private static final boolean CARTESIAN_PRODUCT = false;
	private static final boolean COMMON_ANCESTOR = false;
	private static final boolean CA_join_TC = false;
	private static final boolean MINIMAL_CA = false;
	
	
// fragment
	private static final boolean BOND = true;
	
	
// lowbond initialize
	private static final boolean LowBond = false;
// transitive closure
	private static final boolean TRANSITIVE_CLOSURE = false;
// map fma and snomed ct
	private static final boolean MAP_FMA_SNOMED = false;
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

// configuration for local and hpcc
		FileSystem fs = FileSystem.get(conf);
		
		/*
		 * #################################################################################
		 * #################### ALTERNATIVE FIND UPPER BOUND JOB ###########################
		 * #################################################################################
		 */
		String singelparents_dir = args[1];
		String upper_bound_dir = args[2] + "/ub_tmp";
		String cartesian_product_dir = args[2] + "/cp_tmp";
		String common_ancestor_dir = args[1] + "/ca_tmp";
		String CA_join_TC_dir = args[1] + "/cat_tmp";
		String result_dir = args[2] + "/minimal";
		
		String family = args[1];
		String output = args[2];
		
		
			
// old cartesian product			
//			Job cartesian_product = Job.getInstance(conf, "cartesian_product");
//			cartesian_product.setJarByClass(CartesianInputFormat.class);
//
//			cartesian_product.setOutputKeyClass(Text.class);
//			cartesian_product.setOutputValueClass(Text.class);
//
//			cartesian_product.setMapperClass(Mapper.class);
//			cartesian_product.setNumReduceTasks(0);
//
//			cartesian_product.setInputFormatClass(CartesianInputFormat.class);
//			CartesianInputFormat.setLeftInputInfo(cartesian_product, KeyValueTextInputFormat.class);
//			CartesianInputFormat.setRightInputInfo(cartesian_product, KeyValueTextInputFormat.class);
//			CartesianInputFormat.setSeparator(cartesian_product, "/");
//			CartesianInputFormat.addLeftInputPath(cartesian_product, new Path(upper_bound_dir));
//			CartesianInputFormat.addRightInputPath(cartesian_product, new Path(upper_bound_dir));
//			CartesianInputFormat.setSkipNonSortedOptimizzation(cartesian_product, true);
//			cartesian_product.setOutputFormatClass(TextOutputFormat.class);
//			FileOutputFormat.setOutputPath(cartesian_product, new Path(cartesian_product_dir));
//
//			cartesian_product.waitForCompletion(true);
//		}
		

		
	// for computing fragments	
		if(BOND) {
			fs.delete(new Path(output), true);

			Job Fragment =Job.getInstance(conf,"Fragment");
			for (FileStatus f:fs.listStatus(new Path(family))) {
					DistributedCache.addCacheFile(new Path(family+"/"+f.getPath().getName()).toUri(),Fragment.getConfiguration());
			}
			Fragment.setJarByClass(fragment.class);
			Fragment.setMapperClass(fragment.fragMapper.class);
			Fragment.setReducerClass(fragment.fragReducer.class);
			Fragment.setOutputKeyClass(Text.class);
			Fragment.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(Fragment, new Path(args[0]));
			FileOutputFormat.setOutputPath(Fragment, new Path(output));

			Fragment.waitForCompletion(true);
			
		}
		
	// for computing transtiveclosure	
		if(TRANSITIVE_CLOSURE) {
			fs.delete(new Path(output), true);

			Job transitiveclosure =Job.getInstance(conf,"transitiveclosure");
			for (FileStatus f:fs.listStatus(new Path(family))) {
					DistributedCache.addCacheFile(new Path(family+"/"+f.getPath().getName()).toUri(),transitiveclosure.getConfiguration());
			}
			transitiveclosure.setJarByClass(transitiveClosure.class);
			transitiveclosure.setMapperClass(transitiveClosure.tcMapper.class);
			transitiveclosure.setReducerClass(transitiveClosure.tcReducer.class);
			transitiveclosure.setOutputKeyClass(Text.class);
			transitiveclosure.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(transitiveclosure, new Path(args[0]));
			FileOutputFormat.setOutputPath(transitiveclosure, new Path(output));

			transitiveclosure.waitForCompletion(true);
			
		}

		

		return 1;
	}

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(), new SNOMEDCTDriver(), args);

		System.exit(res);
	}

}
