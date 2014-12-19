#!/bin/bash

# Copyright 2013 BlackBerry, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. 

#What version of Hadoop are we running?
version1=`hadoop version | head -1 | egrep '^Hadop (0\.20|1\.)'`
version2=`hadoop version | head -1 | egrep '^Hadoop 2\.'`

#Determine Hadoop version, and remove the testservice folder if it exists and replace it with the reference one.
if [ version2 != "" ] 
then 
  echo "Running on Hadoop 2"
  hdfs dfs -rm -r /service/99/logsearch-testservice 2>&1 1>/dev/null
  hdfs dfs -put logsearch-testservice /service/99/logsearch-testservice 2>&1 1>/dev/null
elif [ version1 != "" ] 
then 
  echo "Running on Hadoop 1"
  hadoop dfs -rm -r /service/99/logsearch-testservice 2>&1 1>/dev/null
  hadoop dfs -put logsearch-testservice /service/99 2>&1 1>/dev/null
else
  echo "Can't determine Hadoop version."
  exit
fi

#Remove existing output files
rm -rf logsearch-test-output.txt 2> /dev/null
rm -rf logcat-test-output.txt 2> /dev/null
rm -rf loggrep-test-output.txt 2> /dev/null
rm -rf logmultisearch-test-output.txt 2> /dev/null

#Test local sorting
echo "Executing locally sorted test searches."

#Execute remote-sorted searches on test data, dumping results to the output file. 
logtoolsearch -string='test' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00'  > logsearch-test-output.txt
logtoolsearch -string='TEST' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='ä' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='Ä' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --i -string='ä' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='fenêtre' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='FENÊTRE' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --i -string='feNêtRe' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='человек' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='ЧЕЛОВЕК' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --i -string='ЧЕЛовЕК' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='رجل' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --i -string='رجل' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='αβγδε' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='ΑΒΓΔΕ' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --i -string='αβγΔΕ' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='#!A' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --i -string='#!a' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='^X' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --i -string='^x' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='3.14159265358979' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00'  >> logsearch-test-output.txt
logtoolsearch -string='1.602E-19' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='1.602x10^-19' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='123,456,789.00' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch -string='2012-02-28T10:00:01Z' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt

#Compare output of test searches to reference output
echo "****"
logsearchresult=`diff -q logsearch-test-output.txt reference-files/logsearch-reference.txt 2>&1`
if [ "$logsearchresult" = "" ] 
then echo "Logsearch tests successful."
else echo "Logsearch test failed."
fi
echo ""

#Dump the logs from the testservice folder
logtoolcat -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' > logcat-test-output.txt

#Compare cat output to reference file
echo "****"
logcatresult=`diff -eq logcat-test-output.txt reference-files/logcat-reference.txt 2>&1`
if [ "$logcatresult" = "" ]
then echo "Logcat test successful."
else echo "Logcat test failed."
fi
echo ""

#Run some basic grep commands on the test file, ensuring that multi-byte UTF-8 encoded regexs return properly
logtoolgrep --i -regex='^THIS IS A TEST MESSAGE' '99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' > loggrep-test-output.txt
logtoolgrep -regex='^This' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> loggrep-test-output.txt
logtoolgrep -regex='c?n' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> loggrep-test-output.txt
logtoolgrep -regex='c*n' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> loggrep-test-output.txt
logtoolgrep -regex='αβγδε|человек|fenêtre|ä|رجل' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> loggrep-test-output.txt

#Compare grep output to reference file
echo "****"
loggrepresult=`diff -eq loggrep-test-output.txt reference-files/loggrep-reference.txt 2>&1`
if [ "$loggrepresult" = "" ]
then echo "Loggrep test successful."
else echo "Loggrep test failed."
fi
echo ""

#Run a multi search, consisting mostly of special characters.
logtoolmultisearch -strings=logmultisearch-strings-OR.txt -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' > logmultisearch-test-output.txt
logtoolmultisearch --i -strings=logmultisearch-strings-OR.txt -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logmultisearch-test-output.txt
logtoolmultisearch --a -strings=logmultisearch-strings-AND.txt -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logmultisearch-test-output.txt
logtoolmultisearch --a --i -strings=logmultisearch-strings-AND.txt -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logmultisearch-test-output.txt

#Comapre multisearch output to reference file
echo "****"
logmultisearchresult=`diff -eq logmultisearch-test-output.txt reference-files/logmultisearch-reference.txt 2>&1`
if [ "$logmultisearchresult" = "" ]
then echo "Logmultisearch test successful."
else echo "Logmultisearch test failed."
fi

#Test remote sorting
echo ""
echo "Executing remotely sorted test searches."

logtoolsearch --r -string='test' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' > logsearch-test-output.txt
logtoolsearch --r -string='TEST' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='ä' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='Ä' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r --i -string='ä' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='fenêtre' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='FENÊTRE' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r --i -string='feNêtRe' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='человек' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='ЧЕЛОВЕК' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r --i -string='ЧЕЛовЕК' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='رجل' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r --i -string='رجل' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='αβγδε' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='ΑΒΓΔΕ' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r --i -string='αβγΔΕ' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='#!A' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r --i -string='#!a' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='^X' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r --i -string='^x' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='3.14159265358979' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00'  >> logsearch-test-output.txt
logtoolsearch --r -string='1.602E-19' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='1.602x10^-19' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='123,456,789.00' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt
logtoolsearch --r -string='2012-02-28T10:00:01Z' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logsearch-test-output.txt

#Compare output of test searches to reference output
echo "****"
logsearchresult=`diff -q logsearch-test-output.txt reference-files/logsearch-reference.txt 2>&1`
if [ "$logsearchresult" = "" ] 
then echo "Logsearch tests successful."
else echo "Logsearch test failed."
fi
echo ""

#Dump the logs from the testservice folder
logtoolcat --r -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' > logcat-test-output.txt

#Compare cat output to reference file
echo "****"
logcatresult=`diff -eq logcat-test-output.txt reference-files/logcat-reference.txt 2>&1`
if [ "$logcatresult" = "" ]
then echo "Logcat test successful."
else echo "Logcat test failed."
fi
echo ""

#Run some basic grep commands on the test file, ensuring that multi-byte UTF-8 encoded regexs return properly
logtoolgrep --r --i -regex='^THIS IS A TEST MESSAGE' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' > loggrep-test-output.txt
logtoolgrep --r -regex='^This' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> loggrep-test-output.txt
logtoolgrep --r -regex='c?n' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> loggrep-test-output.txt
logtoolgrep --r -regex='c*n' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> loggrep-test-output.txt
logtoolgrep --r -regex='αβγδε|человек|fenêtre|ä|رجل' -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> loggrep-test-output.txt

#Compare grep output to reference file
echo "****"
loggrepresult=`diff -eq loggrep-test-output.txt reference-files/loggrep-reference.txt 2>&1`
if [ "$loggrepresult" = "" ]
then echo "Loggrep test successful."
else echo "Loggrep test failed."
fi
echo ""

#Run a multi search, consisting mostly of special characters.
logtoolmulti --r -strings=logmultisearch-strings-OR.txt -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' > logmultisearch-test-output.txt
logtoolmulti --r --i -strings=logmultisearch-strings-OR.txt -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logmultisearch-test-output.txt
logtoolmulti --r --a -strings=logmultisearch-strings-AND.txt -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' >> logmultisearch-test-output.txt
logtoolmulti --r --a --i -strings=logmultisearch-strings-AND.txt -dc='99' -svc='logsearch-testservice' -comp='logsearch-test' -start='Feb 28, 2012 10:00' -end='Feb 28, 2012 11:00' 	>> logmultisearch-test-output.txt

#Comapre multisearch output to reference file
echo "****"
logmultisearchresult=`diff -eq logmultisearch-test-output.txt reference-files/logmultisearch-reference.txt 2>&1`
if [ "$logmultisearchresult" = "" ]
then echo "Logmultisearch test successful."
else echo "Logmultisearch test failed."
fi
