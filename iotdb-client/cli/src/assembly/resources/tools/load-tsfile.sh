#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

echo ---------------------
echo Start Loading TsFile
echo ---------------------

source "$(dirname "$0")/../sbin/iotdb-common.sh"
#get_iotdb_include and checkAllVariables is in iotdb-common.sh
VARS=$(get_iotdb_include "$*")
checkAllVariables
export IOTDB_HOME="${IOTDB_HOME}"
eval set -- "$VARS"


HELP="Usage: $0 -f <file_path> -cfd <copy_fail_dir> [--sgLevel <sg_level>] [--verify <true/false>] [--onSuccess <none/delete>] [-h <ip>] [-p <port>] [-u <username>] [-pw <password>]"

# Added parameters when default parameters are missing
user_param="-u root"
passwd_param="-pw root"
host_param="-h 127.0.0.1"
port_param="-p 6667"

while true; do
    case "$1" in
        -u)
            user_param="-u $2"
            shift 2
            ;;
        -pw)
            passwd_param="-pw $2"
            shift 2
        ;;
        -h)
            host_param="-h $2"
            shift 2
        ;;
        -p)
            port_param="-p $2"
            shift 2
        ;;
        -f)
            load_dir_param="$2"
            shift 2
        ;;
        -cfd)
            fail_dir_param="$2"
            shift 2
        ;;
        --sgLevel)
            sg_level_param="$2"
            shift 2
        ;;
        --verify)
            verify_param="$2"
            shift 2
        ;;
        --onSuccess)
            on_success_param="$2"
            shift 2
        ;;
        "")
              #if we do not use getopt, we then have to process the case that there is no argument.
              #in some systems, when there is no argument, shift command may throw error, so we skip directly
              break
              ;;
        *)
            echo "Unrecognized options:$1"
            echo "${HELP}"
            exit 0
        ;;
    esac
done

if [ -z "${load_dir_param}" ]; then
    echo "-f option must be set!"
    echo "${HELP}"
fi

echo "start loading TsFiles, please wait..."

#absolute_path_load_dir_param=$(readlink -f $load_dir_param)

LOAD_SQL_PART=" "
if [ -n "${sg_level_param}" ]; then
    LOAD_SQL_PART="${LOAD_SQL_PART} sgLevel=${sg_level_param}"
fi
if [ -n "${verify_param}" ]; then
    LOAD_SQL_PART="${LOAD_SQL_PART} verify=${verify_param}"
fi
if [ -n "${on_success_param}" ]; then
    LOAD_SQL_PART="${LOAD_SQL_PART} onSuccess=${on_success_param}"
fi

PARAMETERS_PART="$host_param $port_param $user_param $passwd_param $PARAMETERS -e "

IOTDB_CLI_CONF=${IOTDB_HOME}/conf

MAIN_CLASS=org.apache.iotdb.cli.Cli

CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

iotdb_cli_params="-Dlogback.configurationFile=${IOTDB_CLI_CONF}/logback-cli.xml"

traverse_files() {

    local folder="$1"

    for file in "$folder"/*; do
        if [ -f "$file" ]; then
            if [[ $file == *.tsfile ]]; then
               LOAD_SQL="load '$file' ${LOAD_SQL_PART}"
               PARAMETERS="${PARAMETERS_PART} \"${LOAD_SQL}\""
               "$JAVA" $iotdb_cli_params -cp "$CLASSPATH" "$MAIN_CLASS" $PARAMETERS
               exit_code=$?
               if [ $exit_code -ne 0 ]; then
                 if [ ! -z "${fail_dir_param}" ]; then
                    if [ ! -d "${fail_dir_param}" ]; then
                        mkdir -p "${fail_dir_param}"
                    fi
                    cp ${file} ${fail_dir_param}
                 fi
               fi
            fi
        elif [ -d "$file" ]; then
            traverse_files "$file"
        fi
    done
}

if [ -f "$load_dir_param" ]; then
    LOAD_SQL="load '$load_dir_param' ${LOAD_SQL_PART}"
    PARAMETERS="${PARAMETERS_PART} \"${LOAD_SQL}\""
    "$JAVA" $iotdb_cli_params -cp "$CLASSPATH" "$MAIN_CLASS" $PARAMETERS
    exit_code=$?

    if [ $exit_code -ne 0 ]; then
      if [ ! -z "${fail_dir_param}" ]; then
         if [ ! -d "${fail_dir_param}" ]; then
             mkdir -p "${fail_dir_param}"
         fi
         cp ${load_dir_param} ${fail_dir_param}
      fi
    fi
elif [ -d "$load_dir_param" ]; then
    traverse_files "$load_dir_param"
fi

echo "end loading TsFiles"
exit 0
