# mg_ruby

A Ruby Extension for InterSystems **Cache/IRIS** and **YottaDB**.

Chris Munt <cmunt@mgateway.com>  
23 January 2020, M/Gateway Developments Ltd [http://www.mgateway.com](http://www.mgateway.com)

* Current Release: Version: 2.1; Revision 40 - Beta (23 January 2020)
* [Release Notes](#RelNotes) can be found at the end of this document.


## Overview

**mg\_ruby** is an Open Source Ruby extension developed for InterSystems **Cache/IRIS** and the **YottaDB** database.  It will also work with the **GT.M** database and other **M-like** databases.


## Pre-requisites

Ruby installation:

       http://www.ruby-lang.org/

InterSystems **Cache/IRIS** or **YottaDB** (or similar M database):

       https://www.intersystems.com/
       https://yottadb.com/

## Installing mg\_ruby

There are three parts to **mg\_ruby** installation and configuration.

* The Ruby extension (**mg\_ruby.so**).
* The database (or server) side code: **zmgsi**
* A network configuration to bind the former two elements together.

### Building the mg\_ruby extension

**mg\_ruby** is written in standard C.  For Linux systems, the Ruby installation procedure can use the freely available GNU C compiler (gcc) which can be installed as follows.

Ubuntu:

       apt-get install gcc

Red Hat and CentOS:

       yum install gcc

Apple OS X can use the freely available **Xcode** development environment.

Under Windows, Ruby is built with the Open Source **MSYS2** Development kit and if you plan to build **mg\_ruby** from the provided source code it is recommended that you select the pre-built **'Ruby+Devkit'** option when downloading Ruby for Windows.  This package will install Ruby together with all the tools needed to build **mg\_ruby**.

Alternatively, there are pre-built Windows x64 binaries available from:

* [https://github.com/chrisemunt/mg_ruby/blob/master/bin/winx64](https://github.com/chrisemunt/mg_ruby/blob/master/bin/winx64)

The pre-built **mg\_ruby.so** module should be copied to the appropriate location in the Ruby file system.  For example, using an 'out of the box' Ruby 2.7 installation this will be:

       C:\Ruby27-x64\lib\ruby\site_ruby\2.7.0\x64-msvcrt

Having done this, **mg\_ruby** is ready for use.

#### Building the source code
 
Having created a suitable development environment, the Ruby Extension installer can be used to build and deploy **mg\_ruby**.  You will find the setup scripts in the /src directory of the distribution.

UNIX and Windows using the MSYS2 Development Toolkit (most installations):

The commands listed below are run from a command shell.  For Windows, the **MSYS2** command shell provided with the **'Ruby+Devkit'** distribution should be used.  For example, with the 'out of the box' Ruby 2.7 installation this will be found at: _C:\Ruby27-x64\msys64\msys2_.

       ruby extconf.rb
       make
       make install

Windows using the Microsoft Development Toolkit:

       ruby extconf.rb
       nmake
       nmake install


### InterSystems Cache/IRIS

Log in to the Manager UCI and install the **zmgsi** routines held in either **/m/zmgsi\_cache.xml** or **/m/zmgsi\_iris.xml** as appropriate.

       do $system.OBJ.Load("/m/zmgsi_cache.xml","ck")

Change to your development UCI and check the installation:

       do ^%zmgsi

       M/Gateway Developments Ltd - Service Integration Gateway
       Version: 3.2; Revision 5 (17 January 2020)

### YottaDB

The instructions given here assume a standard 'out of the box' installation of **YottaDB** deployed in the following location:

       /usr/local/lib/yottadb/r122

The primary default location for routines:

       /root/.yottadb/r1.22_x86_64/r

Copy all the routines (i.e. all files with an 'm' extension) held in the GitHub **/yottadb** directory to:

       /root/.yottadb/r1.22_x86_64/r

Change directory to the following location and start a **YottaDB** command shell:

       cd /usr/local/lib/yottadb/r122
       ./ydb

Link all the **zmgsi** routines and check the installation:

       do ylink^%zmgsi

       do ^%zmgsi

       M/Gateway Developments Ltd - Service Integration Gateway
       Version: 3.2; Revision 5 (17 January 2020)


Note that the version of **zmgsi** is successfully displayed.


## Setting up the network service (if required)

The network setup described here is only required if TCP based connectivity is to be used to connect your Ruby code to the database, as opposed to the API based approach described later.

The default TCP server port for **zmgsi** is **7041**.  If you wish to use an alternative port then modify the following instructions accordingly.

Ruby code using the **mg\_ruby** methods will, by default, expect the database server to be listening on port **7041** of the local server (localhost).  However, **mg\_ruby** provides the functionality to modify these default settings at run-time.  It is not necessary for the web server/Ruby installation to reside on the same host as the database server.

### InterSystems Cache/IRIS

Start the Cache/IRIS-hosted concurrent TCP service in the Manager UCI:

       do start^%zmgsi(0) 

To use a server TCP port other than 7041, specify it in the start-up command (as opposed to using zero to indicate the default port of 7041).

### YottaDB

Network connectivity to **YottaDB** is managed via the **xinetd** service.  First create the following launch script (called **zmgsi\_ydb** here):

       /usr/local/lib/yottadb/r122/zmgsi_ydb

Content:

       #!/bin/bash
       cd /usr/local/lib/yottadb/r122
       export ydb_dir=/root/.yottadb
       export ydb_dist=/usr/local/lib/yottadb/r122
       export ydb_routines="/root/.yottadb/r1.22_x86_64/o*(/root/.yottadb/r1.22_x86_64/r /root/.yottadb/r) /usr/local/lib/yottadb/r122/libyottadbutil.so"
       export ydb_gbldir="/root/.yottadb/r1.22_x86_64/g/yottadb.gld"
       $ydb_dist/ydb -r xinetd^%zmgsis

Create the **xinetd** script (called **zmgsi_xinetd** here): 

       /etc/xinetd.d/zmgsi_xinetd

Content:

       service zmgsi_xinetd
       {
            disable         = no
            type            = UNLISTED
            port            = 7041
            socket_type     = stream
            wait            = no
            user            = root
            server          = /usr/local/lib/yottadb/r122/zmgsi_ydb
       }

* Note: sample copies of **zmgsi\_xinetd** and **zmgsi\_ydb** are included in the **/unix** directory.

Edit the services file:

       /etc/services

Add the following line to this file:

       zmgsi_xinetd          7041/tcp                        # zmgsi

Finally restart the **xinetd** service:

       /etc/init.d/xinetd restart

## Using mg\_ruby

Ruby programs may refer to, and load, the **mg\_ruby** module using the following directive at the top of the script.

       require 'mg_ruby'
       mg_ruby = MG_RUBY.new()

Having added this line, all methods listed provided by the module can be invoked using the following syntax.

       mg_ruby.<method>

It is not necessary to name your instance as 'mg\_ruby'.  For example, you can have:

       require 'mg_ruby'
       <name> = MG_RUBY.new()

Then methods can be invoked as:

       <name>.<method>


### Connecting the database.

By default, **mg\_ruby** will connect to the server over TCP - the default parameters for which being the database listening locally on port **7041**. This can be modified using the following function.

       mg_ruby.m_set_host(<netname>, <port>, <username>, <password>)

The embedded default are for **mg\_ruby** to connect to 'localhost' listening on TCP Port 7041.

Example:

       mg_ruby.m_set_host("localhost", 7041, "", "")

#### Connecting to the database via its API.

As an alternative to connecting to the database using TCP based connectivity, **mg\_ruby** provides the option of high-performance embedded access to a local installation of the database via its API.

##### InterSystems Caché or IRIS.

Use the following functions to bind to the database API.

       mg_ruby.m_set_uci(<namespace>)
       mg_ruby.m_bind_server_api(<dbtype>, <path>, <username>, <password>, <envvars>, <params>)

Where:

* namespace: Namespace.
* dbtype: Database type ('Cache' or 'IRIS').
* path: Path to database manager directory.
* username: Database username.
* password: Database password.
* envvars: List of required environment variables.
* params: Reserved for future use.

Example:

       mg_ruby.m_set_uci("USER")
       result = mg_ruby.m_bind_server_api("IRIS", "/usr/iris20191/mgr", "_SYSTEM", "SYS", "", "")

The bind function will return '1' for success and '0' for failure.

Before leaving your Ruby application, it is good practice to gracefully release the binding to the database:

       mg_ruby.m_release_server_api()

##### YottaDB

Use the following function to bind to the database API.

       mg_ruby.m_bind_server_api(<dbtype>, <path>, <username>, <password>, <envvars>, <params>)

Where:

* dbtype: Database type (‘YottaDB’).
* path: Path to the YottaDB installation/library.
* username: Database username.
* password: Database password.
* envvars: List of required environment variables.
* params: Reserved for future use.

Example:

This example assumes that the YottaDB installation is in: **/usr/local/lib/yottadb/r122**. 
This is where the **libyottadb.so** library is found.
Also, in this directory, as indicated in the environment variables, the YottaDB routine interface file resides (**zmgsi.ci** in this example).  The interface file must contain the following line:

       ifc_zmgsis: ydb_char_t * ifc^%zmgsis(I:ydb_char_t*, I:ydb_char_t *, I:ydb_char_t*)

Moving on to the Ruby code for binding to the YottaDB database.  Modify the values of these environment variables in accordance with your own YottaDB installation.  Note that each line is terminated with a linefeed character, with a double linefeed at the end of the list.

       envvars = "";
       envvars = envvars + "ydb_dir=/root/.yottadb\n"
       envvars = envvars + "ydb_rel=r1.22_x86_64\n"
       envvars = envvars + "ydb_gbldir=/root/.yottadb/r1.22_x86_64/g/yottadb.gld\n"
       envvars =envvars + "ydb_routines=/root/.yottadb/r1.22_x86_64/o*(/root/.yottadb/r1.22_x86_64/r root/.yottadb/r) /usr/local/lib/yottadb/r122/libyottadbutil.so\n"
       envvars = envvars + "ydb_ci=/usr/local/lib/yottadb/r122/zmgsi.ci\n"
       envvars = envvars + "\n"

       result = mg_ruby.m_bind_server_api("YottaDB", "/usr/local/lib/yottadb/r122", "", "", envvars, "")

The bind function will return '1' for success and '0' for failure.

Before leaving your Ruby application, it is good practice to gracefully release the binding to the database:

       mg_ruby.m_release_server_api()


## Invoking database commands from Ruby script

Before invoking database functionality, the following simple script can be used to check that **mg\_ruby** is successfully installed.

       puts mg_ruby.m_ext_version()

This should return something like:

       M/Gateway Developments Ltd. - mg_ruby: Ruby Gateway to M - Version 2.1.40

Now consider the following database script:

       Set ^Person(1)="Chris Munt"
       Set name=$Get(^Person(1))

Equivalent Ruby code:

       mg_ruby.m_set("^Person", 1, "Chris Munt")
       name = mg_ruby.m_get("^Person", 1);


**mg\_ruby** provides functions to invoke all database commands and functions.


#### Set a record

       result = mg_ruby.m_set(<global>, <key>, <data>)
      
Example:

       result = mg_ruby.m_set("^Person", 1, "Chris Munt")

#### Get a record

       result = mg_ruby.m_get(<global>, <key>)
      
Example:

       result = mg_ruby.m_get("^Person", 1)

#### Delete a record

       result = mg_ruby.m_delete(<global>, <key>)
      
Example:

       result = mg_ruby.m_delete("^Person", 1)


#### Check whether a record is defined

       result = mg_ruby.m_defined(<global>, <key>)
      
Example:

       result = mg_ruby.m_defined("^Person", 1)


#### Parse a set of records (in order)

       result = mg_ruby.m_order(<global>, <key>)
      
Example:

       key = ""
       while ((key = mg_ruby.m_order("^Person", key)) != "")
          puts key + " = " + mg_ruby.m_get("^Person", key)
       end


#### Parse a set of records (in reverse order)

       result = mg_ruby.m_previous(<global>, <key>)
      
Example:

       key = ""
       while ((key = mg_ruby.m_previous("^Person", "")) != "")
          puts key + " = " + mg_ruby.m_get("^Person", key)
       end

## Invocation of database functions

       result = mg_ruby.m_function(<function>, <parameters>)
      
Example:

M routine called 'math':

       add(a, b) ; Add two numbers together
                 quit (a+b)

Ruby invocation:

      result = mg_ruby.m_function("add^math", 2, 3);


## Direct access to InterSystems classes (IRIS and Cache)

#### Invocation of a ClassMethod

       result = mg_ruby.m_classmethod(<class_name>, <classmethod_name>, <parameters>);
      
Example (Encode a date to internal storage format):

        result = mg_ruby.m_classmethod("%Library.Date", "DisplayToLogical", "10/10/2019");

## Resources used by zmgsi

The **zmgsi** server-side code will write to the following global:

* **^zmgsi**: The event Log. 

## License

Copyright (c) 2018-2020 M/Gateway Developments Ltd,
Surrey UK.                                                      
All rights reserved.
 
http://www.mgateway.com                                                  
Email: cmunt@mgateway.com
 
 
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.      


## <a name="RelNotes"></a>Release Notes

### v2.1.40 (23 January 2020)

* Initial Release


