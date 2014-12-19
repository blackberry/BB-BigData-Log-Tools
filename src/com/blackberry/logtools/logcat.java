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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.logdriver.util.CatByTime;

public class logcat extends Configured implements Tool {
	
	static int info = 1;
	static int warn = 2;
	static int error = 3;

	public static void main(String[] argv) throws Exception {
		//Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(), new logcat(), argv);
		System.exit(res);
	}  

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
		// - dc number
		// - service
		// - component
		// - startTime (Something 'date' can parse, or just a time in ms from epoch)
		// - endTime (Same as start)
		// - outputDir
		
		//Indexing for arguments to be passed for Mapreduce
		int dcNum = 0;
		int svcNum = 1;
		int compNum = 2;
		int startNum = 3;
		int endNum = 4;
		int outNum = 5;
		
		//Parsing through user arguments
		String[] args = new String[6];
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
				} else if (arg.startsWith("--dateFormat=")) {
					arg = arg.replace("--dateFormat=", "");
					date_format = arg;
				} else if (arg.startsWith("--fieldSeparator=")) {
					arg = arg.replace("--fieldSeparator=", "");
					field_separator = arg;
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
				} else if (arg.startsWith("--out=")) {
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
		
		if (argcount < 6) {
			System.err.println(";****************************************"
					+ "\n\t\t\t NOT ENOUGH ARGUMENTS\n"
					+ "\n\tUSAGE: logcat [REQUIRED ARGUMENTS] [OPTIONS] (Order does not matter)"
					+ "\n\tREQUIRED ARGUMENTS:"
					+ "\n\t\t-dc=[DATACENTER]	Data Center."
					+ "\n\t\t-svc=[SERVICE]		Service."
					+ "\n\t\t-comp=[COMPONENT]	Component."
					+ "\n\t\t-start=[START]		Start time."
					+ "\n\t\t-end=[END]		End time." 
					+ "\n\tOptions:" 
					+ "\n\t\t--out=[DIRECTORY]      	Desired output directory. If not defined, output to stdout."
					+ "\n\t\t--v                  	Verbose output."
					+ "\n\t\t--r                  	Force remote sort."
					+ "\n\t\t--l                  	Force local sort."
					+ "\n\t\t--dateFormat=[FORMAT]  	Valid formats are RFC822, RFC3164 (zero padded day),"
					+ "\n\t            		        RFC5424 (default), or any valid format string for FastDateFormat."
					+ "\n\t\t--fieldSeparator=X      The separator to use to separate fields in intermediate"
					+ "\n\t                  	        files.  Defaults to 'INFORMATION SEPARATOR ONE' (U+001F)."
					+ "\n\t\t--silent		Output only the data."
					+ "\n\t\t--log		        Save all the logs.\n"
					+ ";****************************************");
			System.exit(1);
		}
		
		//Parse time inputs for start and end of search
		args[startNum]=tools.parseDate(args[startNum]);
		args[endNum]=tools.parseDate(args[endNum]);
		tools.checkTime(args[startNum], args[endNum]);
		
		//Retrieve 'out' argument to determine where output of results should be sent
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
		String tmp = "tmp/logcat-" + RandomStringUtils.randomAlphanumeric(10);
		
		//Set args[outNum] to be temp output directory to be passed onto CatByTime instead of UserInput argument
		args[outNum] = (StringEscapeUtils.escapeJava(tmp) + "/rawlines");

		//Managing console output - deal with --v/--silent
		Logger LOG = LoggerFactory.getLogger(logcat.class);
		tools.setConsoleOutput(local_output, quiet, silent);
		
		//Create temp directory in HDFS to store logsearch logs before sorting
		tools.tmpDirHDFS(quiet, silent, fs, conf, tmp, log);
		
		LogTools.logConsole(quiet, silent, warn, "Gathering logs...");
		LogTools.logConsole(quiet, silent, warn, "Passing Arguments: DC=" + args[dcNum] + " Service=" + args[svcNum]
					+ " Component=" + args[compNum] + " StartTime=" + args[startNum]
					+ " EndTime=" + args[endNum] + " Output=" + out);
				
		//Set standard configuration for running Mapreduce and PIG
		String queue_name = "logsearch";
		
		//Start Mapreduce job
		tools.runMRJob(quiet, silent, conf, D_options, out, LOG, field_separator, queue_name, args, "CatByTime", new CatByTime());

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