# mg_ruby

A Ruby Extension for InterSystems **Cache/IRIS** and **YottaDB**.

Chris Munt <cmunt@mgateway.com>  
23 June 2023, MGateway Ltd [http://www.mgateway.com](http://www.mgateway.com)

* Current Release: Version: 2.3; Revision 44.
* Two connectivity models to the InterSystems or YottaDB database are provided: High performance via the local database API or network based.
* [Release Notes](#RelNotes) can be found at the end of this document.

Contents

* [Overview](#Overview") 
* [Pre-requisites](#PreReq") 
* [Installing mg\_ruby](#Install)
* [Using mg\_ruby](#Using)
* [Connecting to the database](#Connect)
* [Invocation of database commands](#DBCommands)
* [Invocation of database functions](#DBFunctions)
* [Transaction Processing](#TProcessing)
* [Direct access to InterSystems classes (IRIS and Cache)](#DBClasses)
* [License](#License)


## <a name="Overview"></a> Overview

**mg\_ruby** is an Open Source Ruby extension developed for InterSystems **Cache/IRIS** and the **YottaDB** database.  It will also work with the **GT.M** database and other **M-like** databases.


## <a name="PreReq"></a> Pre-requisites 

Ruby installation:

       http://www.ruby-lang.org/

InterSystems **Cache/IRIS** or **YottaDB** (or similar M database):

       https://www.intersystems.com/
       https://yottadb.com/


## <a name="Install"></a> Installing mg\_ruby

There are three parts to **mg\_ruby** installation and configuration.

* The Ruby extension (**mg\_ruby.so**).
* The DB Superserver: the **%zmgsi** routines.
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

### Installing the DB Superserver

The DB Superserver is required for:

* Network based access to databases.

Two M routines need to be installed (%zmgsi and %zmgsis).  These can be found in the *Service Integration Gateway* (**mgsi**) GitHub source code repository ([https://github.com/chrisemunt/mgsi](https://github.com/chrisemunt/mgsi)).  Note that it is not necessary to install the whole *Service Integration Gateway*, just the two M routines held in that repository.

#### Installation for InterSystems Cache/IRIS

Log in to the %SYS Namespace and install the **zmgsi** routines held in **/isc/zmgsi\_isc.ro**.

       do $system.OBJ.Load("/isc/zmgsi_isc.ro","ck")

Change to your development Namespace and check the installation:

       do ^%zmgsi

       MGateway Ltd - Service Integration Gateway
       Version: 4.5; Revision 28 (3 February 2023)


#### Installation for YottaDB

The instructions given here assume a standard 'out of the box' installation of **YottaDB** (version 1.30) deployed in the following location:

       /usr/local/lib/yottadb/r130

The primary default location for routines:

       /root/.yottadb/r1.30_x86_64/r

Copy all the routines (i.e. all files with an 'm' extension) held in the GitHub **/yottadb** directory to:

       /root/.yottadb/r1.30_x86_64/r

Change directory to the following location and start a **YottaDB** command shell:

       cd /usr/local/lib/yottadb/r130
       ./ydb

Link all the **zmgsi** routines and check the installation:

       do ylink^%zmgsi

       do ^%zmgsi

       MGateway Ltd - Service Integration Gateway
       Version: 4.5; Revision 28 (3 February 2023)

Note that the version of **zmgsi** is successfully displayed.

Finally, add the following lines to the interface file (**zmgsi.ci** in the example used in the db.open() method).

       sqlemg: ydb_string_t * sqlemg^%zmgsis(I:ydb_string_t*, I:ydb_string_t *, I:ydb_string_t *)
       sqlrow: ydb_string_t * sqlrow^%zmgsis(I:ydb_string_t*, I:ydb_string_t *, I:ydb_string_t *)
       sqldel: ydb_string_t * sqldel^%zmgsis(I:ydb_string_t*, I:ydb_string_t *)
       ifc_zmgsis: ydb_string_t * ifc^%zmgsis(I:ydb_string_t*, I:ydb_string_t *, I:ydb_string_t*)

A copy of this file can be downloaded from the **/unix** directory of the  **mgsi** GitHub repository [here](https://github.com/chrisemunt/mgsi)


### Starting the DB Superserver

The default TCP server port for **zmgsi** is **7041**.  If you wish to use an alternative port then modify the following instructions accordingly.

* For InterSystems DB servers the concurrent TCP service should be started in the **%SYS** Namespace.

Start the DB Superserver using the following command:

       do start^%zmgsi(0) 

To use a server TCP port other than 7041, specify it in the start-up command (as opposed to using zero to indicate the default port of 7041).

* For YottaDB, as an alternative to starting the DB Superserver from the command prompt, Superserver processes can be started via the **xinetd** daemon.  Instructions for configuring this option can be found in the **mgsi** repository [here](https://github.com/chrisemunt/mgsi)

Ruby code using the **mg\_ruby** functions will, by default, expect the database server to be listening on port **7041** of the local server (localhost).  However, **mg\_ruby** provides the functionality to modify these default settings at run-time.  It is not necessary for the Ruby installation to reside on the same host as the database server.


### Resources used by the DB Superserver (%zmgsi)

The **zmgsi** server-side code will write to the following global:

* **^zmgsi**: The event Log. 


## <a name="Using"></a> Using mg\_ruby

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


## <a name="Connect"></a> Connecting to the database

By default, **mg\_ruby** will connect to the server over TCP - the default parameters for which being the database listening locally on port **7041**. This can be modified using the following function.

       mg_ruby.m_set_host(<netname>, <port>, <username>, <password>)

The embedded default are for **mg\_ruby** to connect to 'localhost' listening on TCP Port 7041.

Example:

       mg_ruby.m_set_host("localhost", 7041, "", "")

### Connecting to the database via its API.

As an alternative to connecting to the database using TCP based connectivity, **mg\_ruby** provides the option of high-performance embedded access to a local installation of the database via its API.

#### InterSystems Caché or IRIS.

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

#### YottaDB

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

This example assumes that the YottaDB installation is in: **/usr/local/lib/yottadb/r130**. 
This is where the **libyottadb.so** library is found.
Also, in this directory, as indicated in the environment variables, the YottaDB routine interface file resides (**zmgsi.ci** in this example).  The interface file must contain the following lines:

       sqlemg: ydb_string_t * sqlemg^%zmgsis(I:ydb_string_t*, I:ydb_string_t *, I:ydb_string_t *)
       sqlrow: ydb_string_t * sqlrow^%zmgsis(I:ydb_string_t*, I:ydb_string_t *, I:ydb_string_t *)
       sqldel: ydb_string_t * sqldel^%zmgsis(I:ydb_string_t*, I:ydb_string_t *)
       ifc_zmgsis: ydb_string_t * ifc^%zmgsis(I:ydb_string_t*, I:ydb_string_t *, I:ydb_string_t*)

Moving on to the Ruby code for binding to the YottaDB database.  Modify the values of these environment variables in accordance with your own YottaDB installation.  Note that each line is terminated with a linefeed character, with a double linefeed at the end of the list.

       envvars = "";
       envvars = envvars + "ydb_dir=/root/.yottadb\n"
       envvars = envvars + "ydb_rel=r1.30_x86_64\n"
       envvars = envvars + "ydb_gbldir=/root/.yottadb/r1.30_x86_64/g/yottadb.gld\n"
       envvars = envvars + "ydb_routines=/root/.yottadb/r1.30_x86_64/o*(/root/.yottadb/r1.30_x86_64/r root/.yottadb/r) /usr/local/lib/yottadb/r130/libyottadbutil.so\n"
       envvars = envvars + "ydb_ci=/usr/local/lib/yottadb/r130/zmgsi.ci\n"
       envvars = envvars + "\n"

       result = mg_ruby.m_bind_server_api("YottaDB", "/usr/local/lib/yottadb/r130", "", "", envvars, "")

The bind function will return '1' for success and '0' for failure.

Before leaving your Ruby application, it is good practice to gracefully release the binding to the database:

       mg_ruby.m_release_server_api()


## <a name="DBCommands"></a> Invocation of database commands

Before invoking database functionality, the following simple script can be used to check that **mg\_ruby** is successfully installed.

       puts mg_ruby.m_ext_version()

This should return something like:

       MGateway Ltd. - mg_ruby: Ruby Gateway to M - Version 2.3.44

Now consider the following database script:

       Set ^Person(1)="Chris Munt"
       Set name=$Get(^Person(1))

Equivalent Ruby code:

       mg_ruby.m_set("^Person", 1, "Chris Munt")
       name = mg_ruby.m_get("^Person", 1);


**mg\_ruby** provides functions to invoke all database commands and functions.


### Set a record

       result = mg_ruby.m_set(<global>, <key>, <data>)
      
Example:

       result = mg_ruby.m_set("^Person", 1, "Chris Munt")

### Get a record

       result = mg_ruby.m_get(<global>, <key>)
      
Example:

       result = mg_ruby.m_get("^Person", 1)

### Delete a record

       result = mg_ruby.m_delete(<global>, <key>)
      
Example:

       result = mg_ruby.m_delete("^Person", 1)


### Check whether a record is defined

       result = mg_ruby.m_defined(<global>, <key>)
      
Example:

       result = mg_ruby.m_defined("^Person", 1)


### Parse a set of records (in order)

       result = mg_ruby.m_order(<global>, <key>)
      
Example:

       key = ""
       while ((key = mg_ruby.m_order("^Person", key)) != "")
          puts key + " = " + mg_ruby.m_get("^Person", key)
       end


### Parse a set of records (in reverse order)

       result = mg_ruby.m_previous(<global>, <key>)
      
Example:

       key = ""
       while ((key = mg_ruby.m_previous("^Person", "")) != "")
          puts key + " = " + mg_ruby.m_get("^Person", key)
       end


### Increment the value of a global node

       result = mg_ruby.m_increment(<global>, <key>, <increment_value>)
      
Example:

       result = mg_ruby.m_increment("^Global", "counter", 1)


## <a name="DBFunctions"> Invocation of database functions

       result = mg_ruby.m_function(<function>, <parameters>)
      
Example:

M routine called 'math':

       add(a, b) ; Add two numbers together
                 quit (a+b)

Ruby invocation:

      result = mg_ruby.m_function("add^math", 2, 3)


## <a name="TProcessing"></a> Transaction Processing

M DB Servers implement Transaction Processing by means of the methods described in this section.

### Start a Transaction

       result = mg_ruby.m_tstart()

* On successful completion this method will return zero, or an error code on failure.

Example:

       result = mg_ruby.m_tstart()


### Determine the Transaction Level

       result = mg_ruby.m_tlevel()

* Transactions can be nested and this method will return the level of nesting.  If no Transaction is active this method will return zero.  Otherwise a positive integer will be returned to represent the current depth of Transaction nesting.

Example:

       tlevel = mg_ruby.m_tlevel()


### Commit a Transaction

       result = mg_ruby.m_tcommit()

* On successful completion this method will return zero, or an error code on failure.

Example:

       result = mg_ruby.m_tcommit()


### Rollback a Transaction

       result = mg_ruby.m_trollback()

* On successful completion this method will return zero, or an error code on failure.

Example:

       result = mg_ruby.m_trollback()


## <a name="DBClasses"> Direct access to InterSystems classes (IRIS and Cache)

### Invocation of a ClassMethod

       result = mg_ruby.m_classmethod(<class_name>, <classmethod_name>, <parameters>)
      
Example (Encode a date to internal storage format):

        result = mg_ruby.m_classmethod("%Library.Date", "DisplayToLogical", "10/10/2019")

### Creating and manipulating instances of objects

The following simple class will be used to illustrate this facility.

       Class User.Person Extends %Persistent
       {
          Property Number As %Integer;
          Property Name As %String;
          Property DateOfBirth As %Date;
          Method Age(AtDate As %Integer) As %Integer
          {
             Quit (AtDate - ..DateOfBirth) \ 365.25
          }
       }

### Create an entry for a new Person

       person =  mg_ruby.m_classmethod("User.Person", "%New");

Add Data:

       result = person.setproperty("Number", 1);
       result = person.setproperty("Name", "John Smith");
       result = person.setproperty("DateOfBirth", "12/8/1995");

Save the object record:

       result = person.method("%Save");

### Retrieve an entry for an existing Person

Retrieve data for object %Id of 1.
 
       person =  mg_ruby.m_classmethod("User.Person", "%OpenId", 1);

Return properties:

       var number = person.getproperty("Number");
       var name = person.getproperty("Name");
       var dob = person.getproperty("DateOfBirth");

Calculate person's age at a particular date:

       today =  mg_ruby.m_classmethod("%Library.Date", "DisplayToLogical", "10/10/2019");
       var age = person.method("Age", today);


## <a name="License"></a> License

Copyright (c) 2018-2023 MGateway Ltd,
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

### v2.1.40a (20 January 2021)

* Restructure and update the documentation.

### v2.2.41 (18 February 2021)

* Introduce support for M transaction processing: tstart, $tlevel, tcommit, trollback.
	* Available with DB Superserver v4 and later. 
* Introduce support for the M increment function.
* Allow the DB server response timeout to be modified via the mg\_ruby.m\_set\_timeout() function.
	* mg_ruby.m\_set\_timeout([timeout])

### v2.2.42 (14 March 2021)

* Introduce support for YottaDB Transaction Processing over API based connectivity.
	* This functionality was previously only available over network-based connectivity to YottaDB.

### v2.3.43 (20 April 2021)

* Introduce improved support for InterSystems Objects for the standard (PHP/Python/Ruby) connectivity protocol.
	* This enhancement requires DB Superserver version 4.2; Revision 19 (or later).

### v2.3.44 (27 October 2021)

* Ensure that data strings returned from YottaDB are correctly terminated.
* Verify that **mg\_ruby** will build and work with Ruby v3.0.x.

### v2.3.44a (23 June 2023)

* Documentation update.