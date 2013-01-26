package com.aczire.sar;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.aczire.sar.compression.GZipLib;
import com.aczire.sar.security.AESCrypter;


public class ArchiveSearcher {
	private static final Log LOG = LogFactory.getLog(ArchiveSearcher.class);

	private String inPath = "";
	private String outPath = "";
	private String unlockKey = "KEY"; // Shell password to encrypt the blocks.
	private String searchKeyword = "";
	private String searchFilename = "";

	private boolean inPathTypeLocal = false; // input path is hdfs.
	private boolean outPathTypeLocal = false; // output path is hdfs.	

	Configuration conf = new Configuration();

	static class SequenceFileMapper
	extends Mapper<SarKey, BytesWritable, SarKey, BytesWritable> {
		
		// Signal the map runner to stop calling map any further.
		private boolean finished = false;
		
		/*
		 * The custom run method ensures that, when searching for only the filename,
		 * the mapreduce job will no longer continue after finding the correct filename. 
		 */
		@Override
		public void run(Context context) throws InterruptedException{
		     try{
		          setup(context);
		          while(context.nextKeyValue() && !finished){
		                 map(context.getCurrentKey(), context.getCurrentValue(), context);
		           }
		           cleanup(context);
		      } catch(EOFException eofe){
		    	  LOG.error("EOFException: Corrupt file.", eofe);
		      } catch (IOException ioe) {
		    	  LOG.error("IOException: Corrupt file.", ioe);
			}
		}

		private void WriteLocal(String outputPath, String filename, byte[] contents) throws IOException{
			//write the file directly to local file system.
			FileUtils.forceMkdir(new File( outputPath));
			FileUtils.deleteQuietly(new File(outputPath + filename));
			FileOutputStream fos = new FileOutputStream(outputPath + filename);
			fos.write(contents);
			fos.close();	
		}

		private void WriteHDFS(Configuration conf, String outputPath, String filename, byte[] contents) throws IOException{
			// Get the underlying HDFS filesystem and the output path.
			
			// TODO: Create custom output formatter for wring to HDFS than writing directly onto it.
			
			FileSystem fs = FileSystem.get(conf);
			Path outDirectory = new Path(outputPath);
			Path outFile = new Path(outputPath + filename);

			// Build the directory tree if not exists already.
			if (fs.exists(outDirectory)){
				LOG.info("Creating directory tree for " + outputPath);
				fs.mkdirs(outDirectory);
			}

			// Delete the file if it already exists.
			if(fs.exists(outFile)){
				LOG.info("Deleting file " + outputPath + filename);
				fs.delete(outFile, true);
			}

			//write the file directly to HDFS file system.
			FSDataOutputStream out = fs.create(outFile);
			out.write(contents, 0, contents.length);

		}

		public void map(SarKey key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String filename = key.Filename.toString();
			String sarKey = context.getConfiguration().get("sar.encrypt.key");
			String searchFilename = context.getConfiguration().get("sar.search.filename");
			String searchKeyword = context.getConfiguration().get("sar.search.keyword");
			String sarOutputPath = context.getConfiguration().get("sar.out.path");
			boolean sarOutPathLocal = Boolean.parseBoolean(context.getConfiguration().get("sar.out.path.local"));
			LOG.info("sar.out.path.local " + context.getConfiguration().get("sar.out.path.local"));

			// If we have the filename constraint, bail out immediately if condition not satisfied.
			if( null != searchFilename && !searchFilename.equals("") )
			{
				if( !filename.equals(searchFilename))
				{
					return;
				}
			}

			byte[] cipherText = value.copyBytes();
			LOG.info("File " + filename + " to decrypt. Length: " + cipherText.length);

			try {
				byte[] plainText = null ;
				if(key.Locked){
					MessageDigest md = MessageDigest.getInstance("sha-256");
					byte[] digestOfPassword = md.digest(sarKey.getBytes("utf-8"));
					String encryptionKey = new String(digestOfPassword);
					if(encryptionKey.equals(key.Key)){
						plainText = (key.Locked) ? AESCrypter.decrypt(cipherText, sarKey) : cipherText;
					}
					else{
						LOG.error("Incorrect password.");
						return;
					}
				}				

				byte[] decompressed;
				decompressed = (key.Compressed) ? GZipLib.decompress(plainText) : plainText;

				LOG.info("File " + filename + " decrypted. Length: " + plainText.length);
				LOG.info("File " + filename + " decompressed. Length: " + decompressed.length);
				if( null != searchKeyword && !searchKeyword.equals("") )
				{
					LOG.info("Searching keyword in " + filename + " for " +  searchKeyword);
					if(new String(decompressed).contains(searchKeyword))
					{
						LOG.info("File " + filename + " Search hit.");
						if(sarOutPathLocal){
							LOG.info("Writing file to local filesystem " + sarOutputPath);
							WriteLocal(sarOutputPath, filename, decompressed);
						}
						else{
							LOG.info("Writing file to hdfs " + sarOutputPath);
							WriteHDFS(context.getConfiguration(), sarOutputPath, filename, decompressed);
						}

						return;
					}
					else						
					{
						LOG.info("File " + filename + " Search miss.");
						return;
					}
				}
				else
				{
					if(sarOutPathLocal == true){
						LOG.info("Writing file to local filesystem @ " + sarOutputPath);
						WriteLocal(sarOutputPath, filename, decompressed);
					}
					else{
						LOG.info("Writing file to hdfs @ " + sarOutputPath);
						WriteHDFS(context.getConfiguration(), sarOutputPath, filename, decompressed);
					}
					
					finished = true;
				}
			} catch (Exception e) {
				LOG.error(e.toString());
			}
		}
	}

	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("Client", opts);
	}	

	public boolean init(String[] args) throws ParseException {
		LOG.info("Initializing archive search client.");
		Options opts = new Options();
		opts.addOption("in_path", true, "Input directory with files.");
		opts.addOption("in_path_local", false, "Specifies the input directory is local filesystem.");
		opts.addOption("out_path", true, "Output directory.");
		opts.addOption("out_path_local", false, "Specifies the output directory is local filesystem.");
		opts.addOption("key", true, "Password used to encrypt files.");
		opts.addOption("search_file", true, "Search for the file <filename>. (case sensitive)");
		opts.addOption("search_keyword", true, "Search for the keyword <keyword>.");
		opts.addOption("help", false, "Print usage information.");

		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException("No args specified for archive searcher to initialize");
		}		

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			return false;
		}

		/*conf.set("mapreduce.output.fileoutputformat.compress", "true");
			conf.set("mapreduce.output.fileoutputformat.compression.type", "BLOCK");
			conf.set("mapreduce.output.fileoutputformat.compress.codec",
		    "org.apache.hadoop.io.compress.GzipCodec");*/
		if (!cliParser.hasOption("in_path")) {
			throw new IllegalArgumentException("No input folder specified.");
		}
		else {
			inPath = cliParser.getOptionValue("in_path");
			if (cliParser.hasOption("in_path_local")) {
				inPathTypeLocal = true;
			}			
		}
		if (!cliParser.hasOption("out_path")) {
			throw new IllegalArgumentException("No output folder specified.");
		}
		else {
			outPath = cliParser.getOptionValue("out_path");
			if (cliParser.hasOption("out_path_local")) {
				outPathTypeLocal = true;
			}			
		}		

		if (cliParser.hasOption("search_file")) {
			searchFilename = cliParser.getOptionValue("search_file");
		}
		if (cliParser.hasOption("search_keyword")) {
			searchKeyword = cliParser.getOptionValue("search_keyword");
		}
		if (!cliParser.hasOption("search_file") && !cliParser.hasOption("search_keyword")) {
			throw new IllegalArgumentException("Please specify either search keyword or file to search for.");
		}
		if (cliParser.hasOption("key")) {
			unlockKey = cliParser.getOptionValue("key");
		}

		conf.set("sar.out.path.local", Boolean.toString(outPathTypeLocal));
		conf.set("sar.out.path", outPath);		
		conf.set("sar.in.path.local", Boolean.toString(inPathTypeLocal));
		conf.set("sar.in.path", inPath);
		conf.set("sar.search.filename", searchFilename);
		conf.set("sar.search.keyword", searchKeyword);		
		conf.set("sar.encrypt.key", unlockKey);		

		return true;
	}

	public boolean run() throws IOException, InterruptedException, ClassNotFoundException {
		LOG.info("Starting Client");	
		Job job = new Job(conf);
		job.setJarByClass(ArchiveSearcher.class);
		job.setJobName("Archive Searcher");
		FileInputFormat.setInputPaths(job, new Path(inPath));
		//FileOutputFormat.setOutputPath(job, new Path(outPath));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		/*SequenceFileOutputFormat.setCompressOutput(job, true);
				SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
				SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);*/
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(Reducer.class);
		//job.setNumReduceTasks(0);
		//job.setNumMapTasks(1);

		return(job.waitForCompletion(true) ? true : false);
	}


	public static void main(String[] args) throws Exception {
		boolean result = false;
		try {
			ArchiveSearcher archiveSearcher = new ArchiveSearcher();
			LOG.info("Initializing archive searcher.");
			boolean doRun = archiveSearcher.init(args);
			if (!doRun) {
				System.exit(0);
			}
			result = archiveSearcher.run();
		} catch (Throwable t) {
			LOG.fatal("Error running Client", t);
			System.exit(1);
		}
		if (result) {
			LOG.info("Archive searcher completed successfully");
			System.exit(0);			
		} 
		LOG.error("Archive searcher failed to complete successfully");
		System.exit(2);
	}
}
