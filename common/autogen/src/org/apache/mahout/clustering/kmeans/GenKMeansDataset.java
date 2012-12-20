/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.clustering.kmeans;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.uncommons.maths.random.GaussianGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;
import org.uncommons.maths.random.ContinuousUniformGenerator;
import org.uncommons.maths.random.DiscreteUniformGenerator;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.mahout.common.AbstractJob;

public class GenKMeansDataset extends Configured implements Tool {
//public class GenKMeansDataset extends AbstractJob {

  private static final Log LOG= LogFactory.getLog(GenKMeansDataset.class);

  private static long SAMPLES_PER_FILE = 20000000;

  private static final String TEST_BASE_PATH = "/KMeans";
  private static final String TEST_INPUT_PATH = TEST_BASE_PATH + "/input";
  private static final String TEST_INPUT_PATH_SAMPLES = TEST_INPUT_PATH + "/points";
  private static final String TEST_INPUT_PATH_INITCLUSTERS = TEST_INPUT_PATH + "/clusters";
  

  public static final double[][] simple = { { 1, 1 }, { 2, 1 }, { 1, 2 },
      { 2, 2 }, { 3, 3 }, { 4, 4 }, { 5, 4 }, { 4, 5 }, { 5, 5 } };

  public static List<Vector> samples;

  public static List<Vector> initialCentroids;

  public static List<Vector> getPoints(double[][] raw) {
    List<Vector> points = new ArrayList<Vector>();
    for (int i = 0; i < raw.length; i++) {
      double[] fr = raw[i];
      Vector vec = new RandomAccessSparseVector(fr.length);
      vec.assign(fr);
      points.add(vec);
    }
    return points;
  }
  
  /**
   * Sample Producer. 
   * The samples may be generated randomly using certain distribution, 
   * or obtained from public data set.
   */
  public abstract static class SampleProducer {
    protected FileSystem fs;
    protected JobConf conf;
	protected DistanceMeasure dm = new EuclideanDistanceMeasure();

   /**
    * Produce vector samples and add into vector.
	* 
    * @param target the target list the produced samples are added into
	* @return the number of samples produced
    */
    
	public long produceSamples(List<Vector> target) throws Exception {return 0;}
   /**
    * Produce vector samples and write them directory to file system.
    * This inteface is called when the samples number is too large to be contained in 
    * a List. So write to file directly.
    * @param samplePath The path on filesystem the samples file to be written to 
    * @return the number of samples produced
    */
	public abstract long produceSamples(Path samplePath) throws Exception;

    /**
     * Produce initial cluster centroids and add into provided list.
     * This interface is called after produceSamples is called.
     * @param numClusters The number centroids to be generated
     * @param centroidList the centroids container
     * @return the actual number of centroids produced
     */  
    public abstract int produceInitialCentroids(int numClusters, List<Vector> centoidList) throws Exception;

    /**
     * Produce initial cluster centroids and write the centroids directly to file system.
     * This interface is called after produceSamples is called.
     * @param numClusters The number centroids to be generated
     * @param centroidsPath the path on FileSystem where centroids are to be written to
     * @return the actual number of centroids produced
     */
    public int produceInitialCentroids(int numClusters, Path centroidsPath) throws Exception {
        List<Vector> iCentroids = new ArrayList<Vector>(numClusters); 
        produceInitialCentroids(numClusters, iCentroids);
        SequenceFile.Writer writer = new SequenceFile.Writer(getFileSystem(), getJobConf(), centroidsPath, Text.class, Kluster.class);
        for (int i = 0; i < iCentroids.size(); i++) {
            Vector vec = iCentroids.get(i);
            Kluster cluster = new Kluster(vec,i,dm);
            // add the center so the centroid will be correct upon output
            cluster.observe(cluster.getCenter(),1);
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
        writer.close();
        return iCentroids.size();
    }

    public void setFileSystem(FileSystem fs, JobConf conf){
        this.fs = fs; 
        this.conf = conf;
    }

    public FileSystem getFileSystem(){
        return this.fs;
    }

    public JobConf getJobConf(){
        return this.conf;
    }

	protected SequenceFile.Writer createNewFile(Path filepath) 
	  throws IOException {
		return createNewFile(filepath, LongWritable.class,VectorWritable.class);
    }

	protected SequenceFile.Writer createNewFile(Path filepath, Class<? extends WritableComparable> keyClass, Class<? extends Writable> valueClass) throws IOException {
		SequenceFile.Writer writer = new SequenceFile.Writer(getFileSystem(), getJobConf(), filepath, keyClass, valueClass);
        LOG.info("creating file "+filepath.toString());
		return writer;
  } 

 }
  
   /**
   * Simple Demo Sample Producer for original samples. 
   * The samples may be generated randomly using certain distribution, 
   * or obtained from public data set.
   */
  public static class DemoSampleProducer extends SampleProducer {
    public DemoSampleProducer() {}
	public long produceSamples(Path samplePath) throws Exception { return 0;}
    public int produceInitialCentroids(int numClusters, List<Vector> centroidList) throws Exception {return 0;}
 }


 /**
  * Generate random sample points using Normal (Gaussian) Distribution.
  */
  public static class GaussianSampleGenerator extends SampleProducer {
	private MersenneTwisterRNG rng;
	private long numSamples = 10;
	private int dimension = 2;
	private double [][][] genParams = { { {0,1}, {0,1} } } ;	
	private double cMin = 0.0;
    private double cMax = 1.0;


	public static class MapClass extends MapReduceBase implements
        Mapper<IntWritable,Text,LongWritable,VectorWritable> {
		private int dimension = 2;

		public void configure(JobConf jobConf){
			this.dimension = Integer.parseInt(jobConf.get("genkmeansdataset.dimensions"));

		}
		public void map( IntWritable key,
                 Text value,
                 OutputCollector<LongWritable,VectorWritable> output,
                 Reporter reporter) throws IOException {

            try {
				MersenneTwisterRNG rng = new MersenneTwisterRNG();
				//create gussian generators based on seeds 
				GaussianGenerator [] gg = new GaussianGenerator [dimension];
				String[] numbers = value.toString().split("\t");
				int i = 0;
				long numSamples = Long.parseLong(numbers[i++]);
				for (int d = 0; d< dimension; d++) {
					double mean = Double.parseDouble(numbers[i++]);
					double std = Double.parseDouble(numbers[i++]);
					LOG.info("dimension="+d+": mean="+mean+", std="+std);
					gg[d] = new GaussianGenerator(mean,std,rng);
            	}

				//generate samples
            	double [] vec = new double[dimension];
            	for(long count = 0; count <numSamples; count++){
                	for(int d = 0; d<dimension ; d++)
                    	vec[d] = gg[d].nextValue();
                	Vector p = new RandomAccessSparseVector(dimension);
                	p.assign(vec);
                	output.collect(new LongWritable(count), new VectorWritable(p));
					reporter.setStatus(Long.toString(count+1)+" samples generated");
					reporter.incrCounter(HiBench.Counters.BYTES_DATA_GENERATED,
							8+p.getNumNondefaultElements()*8);
            	}
			} catch (Exception e) {
                LOG.warn("Exception in GussianSampleGenerator.MapClass");
                e.printStackTrace();
            }
        }
    }


	public GaussianSampleGenerator() {
		rng = new MersenneTwisterRNG();
	}
	public GaussianSampleGenerator(byte[] seed){
		rng = new MersenneTwisterRNG(seed);
	}
  
	public void setGenParams(long num, int dimen, double [][][] params, double cMin, double cMax){
		this.dimension = dimen;
		this.genParams = params;
		this.numSamples = num;
        this.cMin = cMin;
        this.cMax = cMax;
    }

    public long produceSamples(List<Vector> target) throws Exception {
	    long numTotal = this.numSamples;
		int centriodNum = genParams.length;
	    int numPerCluster = (int)Math.ceil((double)numTotal/(double)centriodNum);
		LOG.info("Cluster number="+centriodNum+" numbers per cluster="+numPerCluster);	
		GaussianGenerator [] gg = new GaussianGenerator [dimension];
		for (int k= 0; k<genParams.length; k++){
			if(genParams[k].length != dimension) 
				throw new Exception("The dimension of mean vector or std vector does not match desired dimension!");
			for (int d = 0; d<dimension; d++) {
				if(genParams[k][d].length != 2) throw new Exception("The dimension of mean vector or std vector does not match desired dimension");
				gg[d] = new GaussianGenerator(genParams[k][d][0],genParams[k][d][1],rng);
			}
			double [] vec = new double[dimension];
			for(int i = 0; i<numPerCluster; i++){
				for(int d = 0; d<dimension ; d++)
					vec[d] = gg[d].nextValue();
				Vector p = new RandomAccessSparseVector(dimension);
        	    p.assign(vec);
				target.add(p);
			}
		}
		return numPerCluster*centriodNum;
	
	}

	public long writeSeeds(Path sampleSeedPath) throws Exception {

		int fileNo = 0;
		long numTotal = this.numSamples; //total number of samples
		int centriodNum = genParams.length; // number of initial centroids
        long numPerCluster = (long)Math.ceil(numTotal/(double)centriodNum);
		long numFiles = (long)Math.ceil(numPerCluster/(double)SAMPLES_PER_FILE); //num of files per cluster
		for (int k= 0; k<genParams.length; k++){
           	if(genParams[k].length != dimension)
                throw new Exception("The dimension of mean vector or std vector does not match desired dimension!");

			StringBuilder sb = new StringBuilder();
           	for (int d = 0; d<dimension; d++) {
                if(genParams[k][d].length != 2) throw new Exception("The dimension of mean vector or std vector does not match desired dimension");
			    sb.append("\t"+Double.toString(genParams[k][d][0])+"\t"+Double.toString(genParams[k][d][1]));
           	}
			for (long i = 0; i< numFiles; i++){
		   		SequenceFile.Writer out = createNewFile(new Path(sampleSeedPath,"seed"+(fileNo++)),IntWritable.class,Text.class);
		   		out.append(new IntWritable(k), new Text(Long.toString(SAMPLES_PER_FILE)+sb.toString()));
				out.close();
			}
			if (numFiles*SAMPLES_PER_FILE < numPerCluster){
				long left = numPerCluster-numFiles*SAMPLES_PER_FILE;
				SequenceFile.Writer out = createNewFile(new Path(sampleSeedPath,"seed"+(fileNo++)),IntWritable.class,Text.class);
				out.append(new IntWritable(k), new Text(Long.toString(left)+sb.toString()));
				out.close();
			}
  	 	}
		return numPerCluster*centriodNum;
    }

    public long produceSamples(Path samplePath) throws Exception {
        Path input = new Path(samplePath.toString()+"-seeds");
		this.numSamples = writeSeeds(input);
        LOG.info("Generating "+this.numSamples+" of samples");

		JobConf jobConf = getJobConf();
		jobConf.set("genkmeansdataset.dimensions",Integer.toString(dimension));

		FileInputFormat.setInputPaths(jobConf, input);
  		FileOutputFormat.setOutputPath(jobConf, samplePath);

  		jobConf.setMapperClass(MapClass.class);
  		
  		jobConf.setInputFormat(SequenceFileInputFormat.class);
  		jobConf.setOutputFormat(SequenceFileOutputFormat.class);
  		jobConf.setOutputKeyClass(LongWritable.class);
  		jobConf.setOutputValueClass(VectorWritable.class);		
		jobConf.setNumReduceTasks(0);
		JobClient.runJob(jobConf);		

        return this.numSamples;
    }

    public int produceInitialCentroids(int numClusters, List<Vector> iCentroids) throws Exception { 
	    //create iniital centroids
        ContinuousUniformGenerator ug = new ContinuousUniformGenerator(this.cMin,this.cMax,rng);
    	double [] vec =  new double [dimension];
		for (int k = 0; k<numClusters; k++) {
            for (int d = 0; d<dimension; d++) {
                vec[d] = ug.nextValue();
            }
		    Vector p = new RandomAccessSparseVector(dimension);
		    p.assign(vec);
			iCentroids.add(p);
		}
        return numClusters;
    }
    
  }

 /**
  * Read sample data from dataset file
  */
  public static class DatasetSampleReader extends SampleProducer {
	private String dataFile="";
	private int dimension=2;
	private long numSamples;
	
	public DatasetSampleReader(String datafile){
		this.dataFile = datafile;
	}

	public long produceSamples(Path samplePath) throws Exception{
		//open the data set file, must conform to 1990 Concensus Data format
        if(dataFile.equals("")) {
			throw new IOException("input data set does not exist!");
		}

		BufferedReader in = new BufferedReader(new FileReader(dataFile));	
		String header = in.readLine();
		LOG.info("dataset file features :"+header);
		String[] h = header.split(",");
		//filter out the caseid
		LOG.info("dataset dimension = "+String.valueOf(h.length-1));
		this.dimension = h.length-1;

		long sampleNum = 0;
		long samplesInCurrFile=0;
		String s;
		SequenceFile.Writer out;
        int fileNo = 0;
		out = createNewFile(new Path(samplePath,"file"+(fileNo++)));
		double[] vec = new double [this.dimension];
		while ((s=in.readLine()) != null){
			//parse input line
			String[] elements = s.split(",");
			if(elements.length!=(dimension+1)) throw new Exception("One of input sample does not have correct dimension"+s);
			for (int i=0; i<dimension; i++){
				vec[i] = Double.parseDouble(elements[i+1]);
			}
			//write to output
			Vector p = new RandomAccessSparseVector(dimension);
        	p.assign(vec);
			if (samplesInCurrFile >= SAMPLES_PER_FILE) {
				out.close();
				out = createNewFile(new Path(samplePath,"file"+(fileNo++)));
				samplesInCurrFile = 0;
			}
			out.append(new LongWritable(samplesInCurrFile++), new VectorWritable(p));
			sampleNum ++;
			//LOG.info("writing sample "+samplesInCurrFile);
		}
		out.close();
		in.close();
		LOG.info("Parsed "+String.valueOf(sampleNum)+" samples totally.");
        this.numSamples = sampleNum;
		return this.numSamples;
	}

    public int produceInitialCentroids(int numClusters, List<Vector> iCentroids) throws Exception{
		MersenneTwisterRNG rng = new MersenneTwisterRNG();
		int numMax = (numSamples >= Integer.MAX_VALUE)? Integer.MAX_VALUE:((int)numSamples);
		DiscreteUniformGenerator dug = new DiscreteUniformGenerator(1,numMax,rng);
        
		ArrayList<Integer> indexes  = new ArrayList<Integer>(numClusters);
		for (int i=0; i<numClusters; i++){
			//check duplications
			while(true){
				Integer v = dug.nextValue();
				int j;
				for(j=0; j<indexes.size(); j++)
					if(indexes.get(j)==v) break;	
				if(j==indexes.size()) break;
			}
			indexes.add(dug.nextValue());
		} 

		BufferedReader in = new BufferedReader(new FileReader(dataFile));	
		int line = 0;
		double[] vec = new double[dimension];
		String s;
		while((s=in.readLine())!=null) {
			line++;
			if(indexes.size() == 0) break;
			for (int i = 0; i<indexes.size(); i++){
				if(line == indexes.get(i)){
					LOG.info("found line "+String.valueOf(line));
					String[] elements = s.split(",");
					for (int d=0; d<dimension; d++){
						vec[d] = Double.parseDouble(elements[d+1]);
					}
					Vector p = new RandomAccessSparseVector(dimension);
        			p.assign(vec);
					iCentroids.add(p);
					indexes.remove(i);
					break;
				}
			}
		}
		in.close();
		//dump centroids
		LOG.info("Dumping "+iCentroids.size()+" centroids..");
		for(int i = 0; i<iCentroids.size(); i++){
			LOG.info("Centroid :"+(iCentroids.get(i).asFormatString()));
		}
        return iCentroids.size();
    }


  }


  public int run(String [] args) throws Exception {
	    long numSamples = 20;
		int numClusters = 2;
		int dimension = 2;
		double meanMin = 0;
		double meanMax = 1000;
		double stdMin = -100;
		double stdMax = 100;
		String datasetFile="";
		String sampleDir = TEST_INPUT_PATH_SAMPLES;
		String clusterDir = TEST_INPUT_PATH_INITCLUSTERS;
		String compressCodec="org.apache.hadoop.io.compress.DefaultCodec";
		String compress="false";
		String compressType="BLOCK";

		String welcomeMsg = "Generating Mahout KMeans Input Dataset";
		String usage = "Usage: org.apache.mahout.clustering.kmeans.GenKMeansDataset -sampleDir sampleDirectory -clusterDir centroidDirectory -numClusters numberofClusters -numSamples numberOfSamples -samplesPerFile numberOfSamplesPerFile -sampleDimension dimensionOfEachSample [ -centroidMin minValueOfEachDimensionForCenters -centroidMax maxValueOfEachDimensionForCenters -stdMin minStandardDeviationOfClusters -stdMax maxStandardDeviationOfClusters -maxIteration maxIter (The samples are generated using Gaussian Distribution around a set of centers which are also generated using UniformDistribution";
		System.out.println(welcomeMsg);
		if(args.length == 0) {
			System.err.println(usage);
			System.exit(-1);
		}
		for (int i=0; i<args.length; i++) {
			if(args[i].startsWith("-numSamples")) {
				numSamples = Long.parseLong(args[++i]);
			} else if (args[i].startsWith("-sampleDir")){
				sampleDir = args[++i];
			} else if (args[i].startsWith("-clusterDir")){
				clusterDir = args[++i];
			} else if (args[i].startsWith("-numClusters")) {
				numClusters = Integer.parseInt(args[++i]);
			} else if (args[i].startsWith("-samplesPerFile")) {
				SAMPLES_PER_FILE = Long.parseLong(args[++i]);
			} else if (args[i].startsWith("-sampleDimension")){
				dimension=Integer.parseInt(args[++i]);
			} else if (args[i].startsWith("-centroidMin")) {
				meanMin = Double.parseDouble(args[++i]);
			} else if (args[i].startsWith("-centroidMax")) {
				meanMax = Double.parseDouble(args[++i]);
			} else if (args[i].startsWith("-stdMin")) {
				stdMin = Double.parseDouble(args[++i]);
			} else if (args[i].startsWith("-stdMax")) {
				stdMax = Double.parseDouble(args[++i]);
			} else if (args[i].startsWith("-datasetFile")) {
				datasetFile = args[++i];
            } else if (args[i].startsWith("-maxIteration")) {
			} else if (args[i].startsWith("-compressCodec")){
				compressCodec = args[++i];
			} else if (args[i].startsWith("-compressType")){
				compressType=args[++i];
			} else if (args[i].startsWith("-compress")){
				compress = args[++i];
			} else {
				LOG.warn("Illegal format for parameter : "+args[i]);
			} 
		}

		SampleProducer sp = new DemoSampleProducer();
				
	    //if no dataset input, use random generator
        if(datasetFile.equals("")) {
			LOG.info("KMeans Clustering Input Dataset : Synthetic");	
	        GaussianSampleGenerator gsg = new GaussianSampleGenerator();
            MersenneTwisterRNG rng = new MersenneTwisterRNG();
	   	    ContinuousUniformGenerator ug = new ContinuousUniformGenerator(meanMin,meanMax,rng);
            ContinuousUniformGenerator ugStd = new ContinuousUniformGenerator(stdMin,stdMax,rng);
	                
            //generate samples
		    double [][][] genParams = new double [numClusters][dimension][2];
		    for (int k = 0; k<numClusters; k++) {
                for (int d=0; d<dimension; d++){
    		        genParams[k][d][0] = ug.nextValue(); //the d-th dimension of mean k
	    	        genParams[k][d][1] = ugStd.nextValue(); //the stddev of d-th dimension of mean k
                }
            } 

	   	    LOG.info("Successfully generated Sample Generator Seeds");
   		    LOG.info("samples seeds are: ");
		    for (int k =0; k<numClusters; k++){
		        StringBuffer vec = new StringBuffer("(");
			        for (int d=0; d<dimension; d++){
				        vec.append(genParams[k][d][0]+",");
				    }
				    vec.setCharAt(vec.length()-1,')');
				    StringBuffer vecStd = new StringBuffer("(");
				    for (int d=0; d<dimension; d++){
				        vecStd.append(genParams[k][d][1]+",");
			        }
			        vecStd.setCharAt(vecStd.length()-1,')');
			        LOG.info("mean: "+vec.toString()+" std: "+vecStd.toString());
		    }
   			gsg.setGenParams(numSamples,dimension,genParams,meanMin, meanMax);

			sp = gsg;

		} else {

			LOG.info("KMeans Clustering Input Dataset : from file "+datasetFile);
			// if dataset file is provided, use dataset file as input
			initialCentroids = new ArrayList<Vector>(numClusters);
			DatasetSampleReader dp = new DatasetSampleReader(datasetFile);
			sp = dp;
		}
 
	    JobConf jobConf = new JobConf(getConf(),GenKMeansDataset.class);
		jobConf.set("mapred.output.compress",compress);
		jobConf.set("mapred.output.compression.type",compressType);
		jobConf.set("mapred.output.compression.codec",compressCodec);
		LOG.info("mapred.output.compression.codec=" + jobConf.get("mapred.output.compression.codec"));
	    FileSystem fs = FileSystem.get(jobConf);

        sp.setFileSystem(fs,jobConf);

	    LOG.info("Generate K-Means input Dataset : samples = " + numSamples + "sample dimension" + dimension +" cluster num =" + numClusters);

        //delete input and output directory
	    fs.delete(new Path(sampleDir));
	    LOG.info("Start producing samples...");
        long sampleNum = sp.produceSamples(new Path(sampleDir));
        LOG.info("Finished producing samples");
        // pick k initial cluster centers at random
        fs.delete(new Path(clusterDir));
        LOG.info("Start generating initial cluster centers ..."); 
        sp.produceInitialCentroids(numClusters, new Path(clusterDir,"part-00000"));
		LOG.info("Finished generating initial cluster centers");
		return 0;
  }

 public static void main(String[] args) throws Exception {
   int res = ToolRunner.run(new Configuration(), new GenKMeansDataset(), args);
   System.exit(res);
 }

}

