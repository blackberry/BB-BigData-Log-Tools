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

package com.blackberry.logdriver.mapreduce.boom;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.blackberry.logdriver.boom.LogLineData;

public class BoomIndividualInputFormat extends
    FileInputFormat<LogLineData, Text> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return true;
  }

  @Override
  public RecordReader<LogLineData, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new BoomIndividualRecordReader((FileSplit) split, context);
  }

}
