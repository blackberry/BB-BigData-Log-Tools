package com.blackberry.logtools;
/** Copyright (c) 2014 BlackBerry Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.io.output.TeeOutputStream;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.slf4j.Logger;

import com.google.common.io.Files;

public class LogTools {

	static int info = 1;
	static int warn = 2;
	static int error = 3;
	
	//General suggestions for diagnosing errors
	public String generalError() {
		return "\n\tPlease verify the DC and service names, as well" 
				+ "\n\tas that logs exist during the specified time range."
				+ "\n\tUse the \"hdfs dfs -ls /service\" command to get started."
				+ "\n\n\tAlso note that logsearch tools cannot be run as the HDFS user."
				+ "\n\n\tFor more detailed error output, re-run your command with the -v flag.\n";
	}
	
	//Parsing --v option
	//Check to make sure silent is not set and return the new value for verbose flag
	public boolean parseV(boolean silent) {
		if (silent) {
			logConsole(true, true, error, "Cannot force both Verbose and Silent.");
			System.exit(1);
		}
		return false;
	}
	
	//Parsing --silent option
	//Check to make sure silent is not set and return the new value for verbose flag
	public boolean parseSilent(boolean quiet) {
		if (!quiet) {
			logConsole(true, true, error, "Cannot force both Verbose and Silent.");
			System.exit(1);
		}
		return true;
	}
	
	//Check to make sure both forceremote and forcelocal is not set and return the new value for verbose flag
	public boolean parsePigMode(boolean other) {
		if (other) {
			logConsole(true, true, error, "Cannot force both Remote and Local Sorting.");
			System.exit(1);
		}
		return true;
	}
	
	//Parsing --out= option
	//User inputs output directory that is to be created
	//Check to see if parent directory exists && new output directory does not exist
	public String parseOut(String arg, FileSystem fs) throws Exception, Exception {
		arg = arg.replace("--out=", "");
		if (!fs.exists((new Path(arg)).getParent())) {
			logConsole(true, true, error, "Parent of specified path does not exist.");
			System.exit(1);
		}
		if (fs.exists(new Path(arg))) {
			logConsole(true, true, error, "Please specify a non-existing directory to create and output results.");
			System.exit(1);
		}
		return arg;
	}
	
	//Parse time input to convert to epoch time
	//Error check for valid date and valid epoch time
	public String parseDate(String time) throws Exception {
		String parsedTime = time;
		if (Pattern.matches("[0-9]+", time) == false) {
			String[] dateCmd = {"/bin/sh", "-c", "date -d '" + time + "' +%s"};
			ProcessBuilder pBuilder = new ProcessBuilder(dateCmd);
			pBuilder.redirectErrorStream(true);
			Process p = pBuilder.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            parsedTime = br.readLine() + "000";
			if (parsedTime.contains("invalid date")) {
				logConsole(true, true, error, "Could not parse start time, invalid date.");
				System.exit(1);
			}
		} else if (time.length() != 13) {
			logConsole(true, true, error, "Could not parse start time.");
			logConsole(true, true, error, "Epoch date time must be in miliseconds. (13 digits)");
			System.exit(1);
		}
		return parsedTime;
	}
	
	//Check if start time is before end time
	public void checkTime(String start,String end) {
		if (Long.parseLong(end) < Long.parseLong(start)) {
			logConsole(true, true, error, "End time must be later than start time.");
			System.exit(1);
		}
	}
	
	//Managing console output - deal with --v/--silent
	public void setConsoleOutput(File local_output, boolean quiet, boolean silent) throws Exception {
		//Setting all system outputs to write to local_output file
		FileOutputStream logfile = new FileOutputStream(local_output);
		System.setOut(new PrintStream(new TeeOutputStream(System.out, logfile)));
		System.setOut(new PrintStream(new TeeOutputStream(System.err, logfile)));
		
		//Setting all LOGs to write to local_output file
		Properties prop = new Properties();
		if (!quiet && !silent) {
			prop.setProperty("log4j.rootLogger", "INFO, console, WORKLOG");
			prop.setProperty("log4j.appender.console","org.apache.log4j.ConsoleAppender");
			prop.setProperty("log4j.appender.console.target", "System.err");
			prop.setProperty("log4j.appender.console.layout", "org.apache.log4j.PatternLayout");
			prop.setProperty("log4j.appender.console.layout.ConversionPattern", "%d [%p - %l] %m%n");
		}
		else {
			prop.setProperty("log4j.rootLogger", "INFO, WORKLOG");
		}
		prop.setProperty("log4j.appender.WORKLOG", "org.apache.log4j.FileAppender");
		prop.setProperty("log4j.appender.WORKLOG.File", local_output.toString());
		prop.setProperty("log4j.appender.WORKLOG.layout","org.apache.log4j.PatternLayout");
		prop.setProperty("log4j.appender.WORKLOG.layout.ConversionPattern","%d %c{1} - %m%n");
		
		PropertyConfigurator.configure(prop);
	}
	
		
	//Create temp directory in HDFS to store logsearch logs before sorting
	public void tmpDirHDFS(boolean quiet, boolean silent, FileSystem fs, Configuration conf, String tmp, boolean log) {
				logConsole(quiet,silent,info, "Creating new Temp Directory in HDFS: " + tmp);
				
				try {
					Path path = new Path(tmp);
					if (!(fs.exists(path))) {
						//Create directory
						fs.mkdirs(path);
						if (log != true) {
							fs.deleteOnExit(path);	
						}
					}
				} catch (IOException e) {
					if (e.toString().contains("Failed to find any Kerberos")) {
						logConsole(true,true,error,"No/bad Kerberos ticket - please authenticate.");
						System.exit(1);
					} else if (e.toString().contains("quota") &&  e.toString().contains("exceeded")) {
						logConsole(true,true,error,"Disk quota Exceeded.");
						System.exit(1);
					}
					e.printStackTrace();
					System.exit(1);
				}
	}
	
	//Configure and run Mapreduce job
	public void runMRJob(boolean quiet, boolean silent, Configuration conf, ArrayList<String> D_options, String out,
			Logger LOG, String field_separator, String queue_name, String[] args, String job, Tool tool) throws Exception {
		
		logConsole(quiet, silent, info, "Running Mapreduce job & Calling " + job);
		
		if (out.equals("-")) {
			//Uncompress results to be able to read to stdout
			D_options.add("-Dmapreduce.output.fileoutputformat.compress=false");
		}
		
		try {
		    conf.set("zk.connect.string", System.getenv("ZK_CONNECT_STRING"));
		    conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
		    conf.set("mapred.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
		    conf.setInt("mapred.max.split.size", 256*1024*1024);
		    conf.set("logdriver.output.field.separator", field_separator);
		    conf.set("mapred.job.queue.name", StringEscapeUtils.escapeJava(queue_name));	    
	
		    dOpts(D_options, silent, out, conf);
		    
		    //Now run JOB and send arguments
		 	LOG.info("Sending args to " + job + ": {}", args);
		 	ToolRunner.run(conf, tool, args);
		} catch (IOException e) {
			if (e.toString().contains("Failed to find any Kerberos")) {
				logConsole(true, true, error,"No/bad Kerberos ticket - please authenticate.");
				System.exit(1);
			} else if (e.toString().contains("Permission denied")) {
				logConsole(true, true, error,"Permission denied.");
				System.err.println("; Please go to https://go/itforms and filled out the Hadoop Onboarding Form "
						+ "to get access to the requested data.  Paste the following data into the ticket to help with your request:\n"
						+ "Error Message" + e);
				System.exit(1);
			} else if (e.toString().contains("quota") &&  e.toString().contains("exceeded")) {
				logConsole(true, true, error,"Disk quota Exceeded.");
				System.exit(1);
			}
			logConsole(true, true, error,"\n\tError running mapreduce job." + generalError() + "\n\tCommand stopped");
			e.printStackTrace();
			System.exit(1);
		} 
	}
	
	//Get number of found results
	public long getResults(File local_output) {
		try
		{
			BufferedReader redirectoutput = new BufferedReader(new InputStreamReader(new FileInputStream(local_output)));
			String line;
			while ((line = redirectoutput.readLine()) != null) {
				if (line.matches("(?i).*Map output records.*")) {
					String[] records = line.split("=");
					redirectoutput.close();
					return Long.parseLong(records[1]);
				}
			}
			redirectoutput.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return 0;
	}
	
	//Get size of result file
	public long getSize(long foundresults, String tmp, FileSystem fs) throws Exception {
		if (foundresults == 0) {
			logConsole(true, true, error,"No logs found for the given component(s) and time range.");
			System.exit(1);
		}
		return fs.getContentSummary(new Path(tmp + "/rawlines")).getLength();
	}
	
	//Getting LOGDRIVER_JAR variable for running PIG
	public String getLogdriverJar(String LOGDRIVER_HOME) throws Exception {
		
		File file = new File(LOGDRIVER_HOME);
		File[] listOfFiles = file.listFiles();
		for (int i=0; i<listOfFiles.length; i++) {
			if (listOfFiles[i].toString().contains("logdriver-") && !listOfFiles[i].toString().contains("blackberry")) {
				return listOfFiles[i].getName();
			}
		}
		logConsole(true, true, error,"Error with getLogDriverJar");
		System.exit(1);
		return "Error with getLogDriverJar";
	}
	
	//Initialize Pig Job and call appropriate type of Pig Job (remote/local)
	public void runPig(boolean silent, boolean quiet, long foundresults, long size, String tmp, 
			String out, ArrayList<String> D_options, String queue_name, String date_format, 
			String field_separator, File pig_tmp, FileSystem fs, Configuration conf, boolean forcelocal, boolean forceremote) throws Exception {
		//If type of sorting not forced, then choose either local or remote sorting based on size of results
		if (!forceremote && !forcelocal) {
			if (size > 256*1024*1024) {
				forceremote = true;
			} else {
				forcelocal = true;
			}
		}
		
		if (forceremote) {
			logConsole(quiet, silent, warn, "Found Results=" + foundresults
					+ ". Results are " + (100*size/(1024*1024)/100) 
					+ " MB. Using remote sort...");
		} else {
			logConsole(quiet, silent, warn, "Found Results=" + foundresults
					+ ". Results are " + (100*size/(1024*1024)/100) 
					+ " MB. Using local sort...");
		}
		
		String LOGDRIVER_HOME = System.getenv("LOGDRIVER_HOME");
		
		//Convert field separator to hex used for calling PIG
		char[] chars = new char[field_separator.length()];
		chars = field_separator.toCharArray();
		String field_separator_in_hex = String.format("%1x", (int) chars[0]);
	
		//Add the required parameters for running pig
		Map <String, String> params = new HashMap<String,String>();
		params.put("dateFormat", StringEscapeUtils.escapeJava(date_format));
		params.put("fs", StringEscapeUtils.escapeJava(field_separator_in_hex));

		//Set variables to be used for calling Pig script
		String PIG_DIR = LOGDRIVER_HOME + "/pig";
		
		//Get the list of additional jars we'll need for PIG
		String additional_jars = LOGDRIVER_HOME + "/" + getLogdriverJar(LOGDRIVER_HOME);
		
		if (forceremote) {
			runPigRemote(params, out, tmp, quiet, silent, conf, queue_name, additional_jars, pig_tmp, D_options, PIG_DIR, fs);
		} else {
			runPigLocal(params, out, tmp, quiet, silent, conf, queue_name, additional_jars, pig_tmp, D_options, PIG_DIR, fs);
		}
	}
	
	//Run Pig Remotely
	public void runPigRemote(Map <String, String> params, String out, String tmp, boolean quiet, boolean silent, Configuration conf, 
			String queue_name, String additional_jars, File pig_tmp, ArrayList<String> D_options, String PIG_DIR, FileSystem fs) {
		//Set input parameter for pig job - calling Pig directly
		params.put("tmpdir", StringEscapeUtils.escapeJava(tmp));
		
		//Check for an out of '-', meaning write to stdout
		String pigout;
		if (out.equals("-")) {
			params.put("out", tmp + "/final");
			pigout = tmp + "/final";
		}
		else {
			params.put("out", StringEscapeUtils.escapeJava(out));
			pigout = StringEscapeUtils.escapeJava(out);
		}
			
		try {
			logConsole(quiet,silent,info,"Running PIG Command");
			conf.set("mapred.job.queue.name", queue_name);
			conf.set("pig.additional.jars", additional_jars);
			conf.set("pig.exec.reducers.bytes.per.reducer", Integer.toString(100*1000*1000));
			conf.set("pig.logfile", pig_tmp.toString());
			conf.set("hadoopversion", "23");
			//PIG temp directory set to be able to delete all temp files/directories
			conf.set("pig.temp.dir", tmp);

			//Setting output separator for logdriver
			String DEFAULT_OUTPUT_SEPARATOR = "\t";
			Charset UTF_8 = Charset.forName("UTF-8");
			String outputSeparator = conf.get("logdriver.output.field.separator", DEFAULT_OUTPUT_SEPARATOR);
		    byte[] bytes = outputSeparator.getBytes(UTF_8);
		    if (bytes.length != 1) {
		    	logConsole(true, true, error, "The output separator must be a single byte in UTF-8.");
		        System.exit(1);
		    }
		    conf.set("logdriver.output.field.separator", Byte.toString(bytes[0]));
			
		    dOpts(D_options, silent, out, conf);
			
			PigServer pigServer = new PigServer(ExecType.MAPREDUCE, conf);
			pigServer.registerScript(PIG_DIR + "/formatAndSort.pg", params);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		logConsole(quiet, silent, warn, "PIG Job Completed.");
		if (out.equals("-")) {
			System.out.println(";#################### DATA RESULTS ####################");
			try {
				//Create filter to find files with the results from PIG job
				PathFilter filter=new PathFilter(){
			        public boolean accept(Path file) {
			          return file.getName().contains("part-");
			        }
			      };
				
			    //Find the files in the directory, open and printout results
				FileStatus[] status = fs.listStatus(new Path(tmp + "/final"), filter);
				for (int i=0;i<status.length;i++) {
		            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
		            String line;
		            line=br.readLine();
		            while (line != null) {
		                    System.out.println(line);
		                    line=br.readLine();
		            }
				}
				System.out.println(";#################### END OF RESULTS ####################");
            } catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}	
		} else {
			System.out.println(";#################### Done. Search results are in " + pigout + " ####################");
		}
	}
	
	//Run Pig Locally
	public void runPigLocal(Map <String, String> params, String out, String tmp, final boolean quiet, final boolean silent, Configuration conf, 
			String queue_name, String additional_jars, File pig_tmp, ArrayList<String> D_options, String PIG_DIR, FileSystem fs) 
			throws IllegalArgumentException, IOException {
		//Create temp file on local to hold data to sort
		final File local_tmp = Files.createTempDir(); 
		local_tmp.deleteOnExit();
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					logConsole(quiet,silent,warn,"Deleting tmp files in local tmp");
					delete(local_tmp);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}));
		
		//Set input parameter for pig job
		params.put("tmpdir", local_tmp.toString() + "/" + tmp);
		
		//Check for an out of '-', meaning write to stdout
		String pigout;
		if (out.equals("-")) {
			params.put("out", local_tmp + "/" + tmp + "/final");
			pigout = local_tmp + "/" + tmp + "/final";
		}
		else {
			params.put("out", local_tmp + "/" + StringEscapeUtils.escapeJava(out));
			pigout = StringEscapeUtils.escapeJava(out);
		}
		
		//Copy the tmp folder from HDFS to the local tmp directory, and delete the remote folder
		fs.copyToLocalFile(true, new Path(tmp), new Path (local_tmp + "/" + tmp));
		
		try {
			logConsole(quiet,silent,info,"Running PIG Command");
			conf.set("mapred.job.queue.name", queue_name);
			conf.set("pig.additional.jars", additional_jars);
			conf.set("pig.exec.reducers.bytes.per.reducer", Integer.toString(100*1000*1000));
			conf.set("pig.logfile", pig_tmp.toString());
			conf.set("hadoopversion", "23");
			//PIG temp directory set to be able to delete all temp files/directories
			conf.set("pig.temp.dir", local_tmp.getAbsolutePath());

			//Setting output separator for logdriver
			String DEFAULT_OUTPUT_SEPARATOR = "\t";
			Charset UTF_8 = Charset.forName("UTF-8");
			String outputSeparator = conf.get("logdriver.output.field.separator", DEFAULT_OUTPUT_SEPARATOR);
		    byte[] bytes = outputSeparator.getBytes(UTF_8);
		    if (bytes.length != 1) {
		    	System.err.println(";******************** The output separator must be a single byte in UTF-8. ******************** ");
		        System.exit(1);
		    }
		    conf.set("logdriver.output.field.separator", Byte.toString(bytes[0]));

		    dOpts(D_options, silent, out, conf);
			
			PigServer pigServer = new PigServer(ExecType.LOCAL, conf);
			UserGroupInformation.setConfiguration(new Configuration(false));
			pigServer.registerScript(PIG_DIR + "/formatAndSortLocal.pg", params);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		logConsole(quiet, silent, warn, "PIG Job Completed.");
		
		if (out.equals("-")) {
			System.out.println(";#################### DATA RESULTS ####################");
			try {
				File results = new File(pigout);
			    String[] resultList = results.list();
			   			    
			    //Find the files in the directory, open and printout results
				for (int i=0;i<resultList.length;i++) {
					if (resultList[i].contains("part-") && !resultList[i].contains(".crc")) {
						BufferedReader br = new BufferedReader(new FileReader(new File(pigout + "/" + resultList[i])));
			            String line;
			            line=br.readLine();
			            while (line != null) {
			                    System.out.println(line);
			                    line=br.readLine();
			            }
			            br.close();
					}
				}
				System.out.println(";#################### END OF RESULTS ####################");
            } catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}	
		} else {
			fs.copyFromLocalFile(new Path(local_tmp + "/" + StringEscapeUtils.escapeJava(out)), new Path(pigout));
			System.out.println(";#################### Done. Search results are in " + pigout + " ####################");
		}
	}
	
	//Display location of tmp files if log enabled
	public void logs(boolean log, File local_output, File pig_tmp, String tmp) {
		if (log == true) {
			System.err.println("; Local output log saved in: " + local_output.getPath());
			System.err.println("; Pig tmps log saved in: " + pig_tmp.getPath());
			System.err.println("; HDFS temp file saved in: " + tmp);
		}
		return;
	}
	
	public static void logConsole(boolean quiet, boolean silent, int priority, String msg) {
		if (priority==error) {
			System.err.println(";******************** " + msg + " ********************");
		} else if (!quiet && !silent && priority==info) {
			System.err.println("; " + msg);
		} else if (!silent && priority == warn) {
			System.err.println("; " + msg);
		}
	}

	public static void dOpts ( ArrayList<String> D_options, boolean silent, String out, Configuration conf) {
		//Run overriden D_options
		if (D_options.size() != 0) {
			if (!silent && !(out.equals("-") && D_options.size() == 1)) {
				System.err.println("; Running overriden -D options:");
			}
			for (int opt = 0; opt < D_options.size(); opt++){
				String[] pig_opts = new String[2];
				pig_opts = (D_options.get(opt)).split("=");
				conf.set(pig_opts[0].replace("-D", ""), pig_opts[1]);
			}
		}
	}
	
	//Function to recursively delete a directory
	public static void delete(File file) throws IOException {
		if(file.isDirectory()){
			//directory is empty, then delete it
	    	if(file.list().length==0){
	    		file.delete();
	    	} else{
	    		//list all the directory contents
	        	String files[] = file.list();
	 
	        	for (String temp : files) {
	        		//construct the file structure
	        		File fileDelete = new File(file, temp);
	        		//recursive delete
	        	    delete(fileDelete);
	        	}
	        	
	        	//check the directory again, if empty then delete it
	        	if(file.list().length==0){
	        		file.delete();
	        	}
	    	}
		} else {
			//if file, then delete it
			file.delete();
	    }
	}
	
	//Dos2Unix Conversion - used for logmultisearch
	//Pass in file/directory to convert and file/directory to save
	public static void dosTounix(File f, File dos2unix) throws IOException {
		BufferedReader read = new BufferedReader(new FileReader(f));
		FileWriter fwri = new FileWriter(dos2unix);
		String line;
		while ((line = read.readLine()) != null) {
		    fwri.write(line+"\n");
		}
		fwri.flush();
		fwri.close();
		read.close();
	}
}