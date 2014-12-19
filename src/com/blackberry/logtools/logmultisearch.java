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

/**
 * Cat Logs in a given file set.
 * <p>
 * Usage: [genericOptions] [-Dlogdriver.search.start.time=X] [-Dlogdriver.search.end.time=X] input [input ...] output
 * <p>
 * 
 */

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.logdriver.util.MultiSearchByTime;
import com.google.common.io.Files;

public class logmultisearch extends Configured implements Tool {

	static int info = 1;
	static int warn = 2;
	static int error = 3;
	
	public static void main(String[] argv) throws Exception {
		//Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(), new logmultisearch(), argv);
		System.exit(res);
	}  

	@SuppressWarnings("static-access")
	public int run(String[] argv) throws Exception {	
	
		//Configuring configuration and filesystem to work on HDFS
		final Configuration conf = getConf(); //Configuration processed by ToolRunner
		FileSystem fs = FileSystem.get(conf);
		//Initiate tools used for running search
		LogTools tools = new LogTools();
		
		//Other options
		String date_format = "RFC5424";
		String field_separator = "";
		ArrayList<String> D_options = new ArrayList<String>();
		boolean quiet = true;
		boolean silent = false;
		boolean log = false;
		boolean forcelocal = false;
		boolean forceremote = false;
		
		//The arguments are 
		// - strings
		// - dc number
		// - service
		// - component
		// - startTime (Something 'date' can parse, or just a time in ms from epoch)
		// - endTime (Same as start)
		// - outputDir
		
		//Indexing for arguments to be passed for Mapreduce
		int stringsNum = 0;
		int dcNum = 1;
		int svcNum = 2;
		int compNum = 3;
		int startNum = 4;
		int endNum = 5;
		int outNum = 6;
		
		//Parsing through user arguments
		String[] args = new String[7];
		int count = 0; //Count created to track the parse of all arguments
		int argcount = 0; //Count created to track number of arguments to be passed on
		while (count < argv.length) {
			String arg = argv[count];
			count++;
			if (arg.equals("--")) {
				break;
			}
			else if (arg.startsWith("-")) {
				if (arg.equals("--v")) {
					quiet = tools.parseV(silent);
				} else if (arg.equals("--i")) {
					conf.set("logdriver.search.case.insensitive", "true");
				} else if (arg.equals("--a")) {
					LogTools.logConsole(quiet, silent, warn, "AND searching selected");
					conf.set("logdriver.search.and", "true");
				} else if (arg.startsWith("--dateFormat=")) {
					arg = arg.replace("--dateFormat=", "");
					date_format = arg;
				} else if (arg.startsWith("--fieldSeparator=")) {
					arg = arg.replace("--fieldSeparator=", "");
					field_separator = arg;
				} else if (arg.startsWith("-strings=")) {
					arg = arg.replace("-strings=", "");
					args[stringsNum]=arg;
					argcount++;
				} else if (arg.startsWith("-dc=")) {
					arg = arg.replace("-dc=", "");
					args[dcNum]=arg;
					argcount++;
				} else if (arg.startsWith("-svc=")) {
					arg = arg.replace("-svc=", "");
					args[svcNum]=arg;
					argcount++;
				} else if (arg.startsWith("-comp=")) {
					arg = arg.replace("-comp=", "");
					args[compNum]=arg;
					argcount++;
				} else if (arg.startsWith("-start=")) {
					arg = arg.replace("-start=", "");
					args[startNum]=arg;
					argcount++;
				} else if (arg.startsWith("-end=")) {
					arg = arg.replace("-end=", "");
					args[endNum]=arg;
					argcount++;
				}
				//User inputs output directory that is to be created
				//Check to see if parent directory exists && output directory does not exist
				else if (arg.startsWith("--out=")) {
					args[outNum]=tools.parseOut(arg, fs);
					argcount++;
				} else if (arg.startsWith("-D")) {
					D_options.add(arg);
				} else if (arg.equals("--silent")) {
					silent = tools.parseSilent(quiet);
				} else if (arg.equals("--log")) {
					log = true;
				} else if (arg.equals("--l")) {
					forcelocal = tools.parsePigMode(forceremote);
				} else if (arg.equals("--r")) {
					forceremote = tools.parsePigMode(forcelocal);
				} else {
					LogTools.logConsole(quiet, silent, error, "Unrecognized option: " + arg);
					System.exit(1);
				}
			} else {
				LogTools.logConsole(quiet, silent, error, "Unrecognized option: " + arg);
				System.exit(1);
			}
		}

		//Default output should be stdout represented by "-"
		if (args[outNum] == null) {
			args[outNum] = "-";
			argcount++;
			LogTools.logConsole(quiet, silent, info, "Output set to default stdout.");
		}
		
		if (argcount < 7) {
			System.err.println(";****************************************"
					+ "\n\t\t\t NOT ENOUGH ARGUMENTS\n"
					+ "\n\tUSAGE: logmultisearch [REQUIRED ARGUMENTS] [OPTIONS] (Order does not matter)"
					+ "\n\tREQUIRED ARGUMENTS:"
					+ "\n\t\t-strings=[STRINGS_DIR|STRINGS_FILE|STRING]	String/file/directory of strings to search."
					+ "\n\t\t-dc=[DATACENTER]				Data Center."
					+ "\n\t\t-svc=[SERVICE]					Service."
					+ "\n\t\t-comp=[COMPONENT]	    			Component."
					+ "\n\t\t-start=[START]					Start time."
					+ "\n\t\t-end=[END]					End time." 
					+ "\n\tOptions:" 
					+ "\n\t\t--out=[DIRECTORY]      				Desired output directory. If not defined, output to stdout."
					+ "\n\t\t--v                  				Verbose output."
					+ "\n\t\t--r                  				Force remote sort."
					+ "\n\t\t--l                  				Force local sort."
					+ "\n\t\t--dateFormat=[FORMAT]  				Valid formats are RFC822, RFC3164 (zero padded day),"
					+ "\n\t            		        			RFC5424 (default), or any valid format string for FastDateFormat."
					+ "\n\t\t--fieldSeparator=X     		 		The separator to use to separate fields in intermediate"
					+ "\n\t                  	        			files.  Defaults to 'INFORMATION SEPARATOR ONE' (U+001F)."
					+ "\n\t\t--silent					Output only the data."
					+ "\n\t\t--i                  				Make search case insensitive."
					+ "\n\t\t--a                  				Enable AND searching."
					+ "\n\t\t--log						Save all the logs.\n"
					+ ";****************************************");
			System.exit(1);
		}
		
		//Parse time inputs for start and end of search
		args[startNum]=tools.parseDate(args[startNum]);
		args[endNum]=tools.parseDate(args[endNum]);
		tools.checkTime(args[startNum], args[endNum]);

		//Retrieve 'strings' argument to be able to pass search strings to HDFS
		//Retrieve 'out' argument to determine where output of results should be sent
		String strings = args[stringsNum];
		String out = args[outNum];
		
		//Generate files to temporarily store output of mapreduce jobs and pig logs locally                
		File local_output = File.createTempFile("tmp.", RandomStringUtils.randomAlphanumeric(10));
		if (log != true) {
			local_output.deleteOnExit();	
		}
		File pig_tmp = File.createTempFile("tmp.", RandomStringUtils.randomAlphanumeric(10));
		if (log != true) {
			pig_tmp.deleteOnExit();		
		}
		
		//Name the temp directory for storing results in HDFS
		String tmp = "tmp/logmultisearch-" + RandomStringUtils.randomAlphanumeric(10);
		
		//Set args[stringsNum] to be location of search strings to be used for the Multisearch
		args[stringsNum] = (StringEscapeUtils.escapeJava(tmp) + "/strings");
		
		//Set args[outNum] to be temp output directory to be passed onto MultiSearchByTime instead of UserInput argument
		args[outNum] = (StringEscapeUtils.escapeJava(tmp) + "/rawlines");

		//Managing console output - deal with --v/--silent
		Logger LOG = LoggerFactory.getLogger(logmultisearch.class);
		tools.setConsoleOutput(local_output, quiet, silent);

		//Create temp directory in HDFS to store logsearch logs before sorting
		tools.tmpDirHDFS(quiet, silent, fs, conf, tmp, log);

		//If the strings argument is the path of a file, copy the file to HDFS.
		//If the strings argument is the path of a directory, copy all files in the directory to HDFS.
		//If the strings argument is not a path to a file/directory, write to a newly created file in HDFS.
		try {
			File f = new File(strings);
			if (f.isFile()) {
				LogTools.logConsole(quiet, silent, warn, "Strings input is a File...");
				
				//dos2unix file conversion
				File dos2unix = File.createTempFile("tmp.", RandomStringUtils.randomAlphanumeric(10));
				dos2unix.deleteOnExit();
				tools.dosTounix(f, dos2unix);
				
				//Copy over temp directory into a new directory in HDFS to be used for logmultisearch
				fs.copyFromLocalFile(new Path(dos2unix.getAbsolutePath()), new Path(tmp + "/strings"));
			} else if (f.isDirectory()) {
				LogTools.logConsole(quiet, silent, warn, "Strings input is a Directory...");
				
				//Get list of all files in directory to convert from dos2unix
				String[] fileList = f.list();

				//Create temp directory to store all converted files
				File tempDir = Files.createTempDir();
				tempDir.deleteOnExit();
				
				//Convert all files from dos2unix and write to temp directory
				for (int i=0; i < fileList.length; i++) {
					File dos2unix = File.createTempFile("unix", fileList[i], tempDir);
					dos2unix.deleteOnExit();
					tools.dosTounix(new File(f.getAbsolutePath() + "/" + fileList[i]), dos2unix);
				}
				
				//Copy over temp directory into a new directory in HDFS to be used for logmultisearch
				fs.copyFromLocalFile(new Path(tempDir.getAbsolutePath()), new Path(tmp + "/strings"));
			} else {
				LogTools.logConsole(quiet, silent, warn, "Strings input is a search string...");
				//Make directory and file for strings
				fs.mkdirs(new Path(tmp + "/strings"));
				fs.createNewFile(new Path(tmp + "/strings/strings"));
				//Write search strings to file
				FSDataOutputStream hdfsOut = fs.create(new Path(tmp + "/strings/strings"));
				hdfsOut.writeUTF(strings);
				hdfsOut.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 
		
		LogTools.logConsole(quiet, silent, warn, "Searching...");
		LogTools.logConsole(quiet, silent, warn, "Passing Arguments: SearchStrings=" + strings + " DC=" + args[dcNum] + " Service=" + args[svcNum]
					+ " Component=" + args[compNum] + " StartTime=" + args[startNum] + " EndTime=" + args[endNum] + " Output=" + out);
				
		//Set standard configuration for running Mapreduce and PIG
		String queue_name = "logsearch";
				
		//Start Mapreduce job
		tools.runMRJob(quiet, silent, conf, D_options, out, LOG, field_separator, queue_name, args, "MultiSearchByTime", new MultiSearchByTime());
		
		//Before sorting, determine the number of records and size of the results found
		long foundresults = tools.getResults(local_output);
		long size = tools.getSize(foundresults, tmp, fs);
						
		//Run PIG job if results found
		tools.runPig(silent, quiet, foundresults, size, tmp, out, D_options, queue_name, date_format, field_separator, pig_tmp, fs, conf, forcelocal, forceremote);
	 			
		//Display location of tmp files if log enabled
		tools.logs(log, local_output, pig_tmp, tmp);
		
		return 0;
	}
}