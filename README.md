
![LaraDB](img/lara-logo.png)

[![Build Status](https://travis-ci.org/dhutchis/LaraDB.svg?branch=master)](https://travis-ci.org/dhutchis/LaraDB)

LaraDB is an implementation of the [Lara Algebra](https://arxiv.org/abs/1703.07342) on the [Apache Accumulo](https://accumulo.apache.org/) database. 
[Graphulo](http://graphulo.mit.edu/)-style server-side iterators implement the Lara operators.

Handy Links:

* [Publications](#publications)
* [People](#people)
* [Sponsors](#sponsors)
* [LaraDB Code](#laradb-code)
  * [Directory Structure](#directory-structure)
  * [Build](#build)
  * [Test](#test)
  * [Develop](#develop)
  * **[Code Examples](#examples) -- Get started here!**
  * [Debug](#debug)




LaraDB is implemented in [Kotlin](https://www.kotlinlang.org/), a modern programming language that is fully compatible with Java. You can call Java methods from Kotlin and Kotlin methods from Java.

LaraDB is tested on Accumulo 1.8.

### Publications

* D. Hutchison, B. Howe, and D. Suciu, [LaraDB: A Minimalist Kernel for Linear and Relational Algebra Computation](https://arxiv.org/abs/1703.07342), in SIGMOD Workshop on Algorithms and Systems for MapReduce and Beyond (BeyondMR), ACM, May 2017.
* D. Hutchison, B. Howe, and D. Suciu, [Lara: A key-value algebra underlying arrays and relations](https://arxiv.org/abs/1604.03607), Apr. 2016. arXiv: 1604.03607 \[cs.DB].


### People
UW Lara team:

* [Shana Hutchison](https://www.linkedin.com/in/shanahutchison/)
* [Bill Howe](https://faculty.washington.edu/billhowe/)
* [Dan Suciu](https://homes.cs.washington.edu/~suciu/)

Collaborators:

* [David Maier](http://web.cecs.pdx.edu/~maier/)
* [Tim Mattson](https://www.linkedin.com/in/tim-mattson-70b1774/)
* [Jeremy Kepner](http://www.mit.edu/~kepner/)
* [Vijay Gadepally](https://vijayg.mit.edu/)


### Sponsors

* [National Science Foundation](https://www.nsf.gov/) -- [Graduate Research Fellowship Program](https://www.nsfgrfp.org/)
* [Pacific Northwest National Laboratory](https://www.pnnl.gov/)

![NSF](img/NSF_red.png) &nbsp;&nbsp;&nbsp;
![PNNL](img/pnnl_red.png)



## LaraDB Code

### Directory Structure
The project's organization follows the Maven [Project Object Model](https://maven.apache.org/guides/introduction/introduction-to-the-pom.html).
See [`pom.xml`](pom.xml) for detailed build configuration information.

<pre>
src/
  main/                Main code and resources. Included in output JAR
    kotlin/...         Source code
    resources/         Contents copied into output JAR
      log4j.properties Logging configuration for clients at runtime
  test/                Test code and resources. Not included in output JAR
    kotlin/...         Source code
    resources/         Contents available for tests and examples
      log4j.properties Logging configuration for tests and examples
      data/            Data folder - contains pre-created graphs
        10Ar.txt       Row indices of a SCALE 10 power law matrix, generated from Graph500
        10Ac.txt       Column indices of a SCALE 10 power law matrix, generated from Graph500
        netflow/...    Netflow data; see README inside this folder
        sensor/...     Array of Things sensor data; see README inside this folder

target/
  lara-graphulo-${version}.jar         LaraDB client binaries
  lara-graphulo-${version}-tserver.jar LaraDB server binaries; install this on Accumulo's lib/ext
  lara-graphulo-${version}-all.jar     LaraDB + all referenced code binary; only built with -DBundleAll
                                       Useful for standalone execution. Used in oceanography.

pom.xml               Maven Project Object Model. Defines how to build LaraDB
deploy.sh             Script to deploy LaraDB server binaries to Accumulo
README.md             This file
GraphuloTest.conf.template  Example configuration file for testing

.gitignore            Files and folders to exclude from git
.travis.yml           Travis continuous integration testing
</pre>


### Build
Prerequisite: Install [Maven](https://maven.apache.org/download.cgi).

#### Obtain Graphulo
**LaraDB requires the Graphulo library**.
Run the following to install Graphulo on your system. You can do this in any directory.

```bash
wget https://github.com/Accla/graphulo/archive/master.zip
unzip master.zip
cd graphulo-master
mvn clean install -DskipTests -Dfindbugs.skip
cd ..
```

If using your own standalone Accumulo instance, also install the Graphulo server JAR 
per Graphulo's `deploy.sh` (see Graphulo's README).


#### Build LaraDB
Run `mvn package -DskipTests` to compile and build.
This creates two LaraDB artifacts inside the `target/` sub-directory:

1. `lara-graphulo-${version}.jar`  LaraDB binaries, enough for client usage.
Include this on the classpath of Java client applications that call LaraDB functions. 
2. `graphulo-${version}-tserver.jar` LaraDB + all referenced binaries, for easy server installation.
Include this in the `lib/ext/` directory of Accumulo server installations,
so that the Accumulo instance has everything it needs to instantiate LaraDB code.
You can use the `deploy.sh` script to install this jar if you have the `ACCUMULO_HOME` environmental set.

#### Other Build Options
Run `mvn package -DskipTests -DBundleAll` to create a jar that includes the Accumulo dependencies 
alongside all other dependencies. This should not be used in an Accumulo installation because
the Accumulo classes will conflict with the jars in the Accumulo installation.
It is useful for running standalone programs.

### Test
Tests only run on Unix-like systems.
Test results are stored in the folder `target/surefire-reports`.

If you don't have an Accumulo instance set up, 
you can tun tests in a portable, lightweight [MiniAccumulo][] cluster by executing `mvn test`. 
This cluster is automatically setup and torn down before and after every test class.
You can view MiniAccumulo logs in the folder `target/mini/logs`.

If you do have a standalone Accumulo instance set up,
then you can run the LaraDB tests on the Accumulo instance by creating a config file with the connection information for the Accumulo instance.
Create a new file called `GraphuloTest.conf` based on the template `GrahuloTest.conf.template`
and fill in your Accumulo's connection information as follows:

    accumulo.it.cluster.standalone.admin.principal=USERNAME
    accumulo.it.cluster.standalone.admin.password=PASSWORD
    accumulo.it.cluster.standalone.zookeepers=ZK_ADDRESSES
    accumulo.it.cluster.standalone.instance.name=INSTNACE_NAME

You can change the filename to something other than `GraphuloTest.conf` if you wish.
In that case, run tests using `mvn test -DTEST_CONFIG=<FILEPATH>` replacing `FILEPATH` with the path to your config file.
You can also specify these parameters directly on the command line, as in 
`mvn test -Daccumulo.it.cluster.standalone.admin.principal=bla -Daccumulo.it.cluster.standalone.admin.password=bla ...`

When running on a full Accumulo instance, if a test fails for any reason, 
it may leave a test table on the Accumulo instance which could mess up future tests.  
Delete the tables manually if this happens.

Run `mvn clean` to delete output from previously run tests.

[MiniAccumulo]: https://accumulo.apache.org/1.8/accumulo_user_manual.html#_mini_accumulo_cluster


### Develop
If you're interested in using an IDE for development, [IntelliJ](https://www.jetbrains.com/idea/) is a good choice.
You can use the free Community Edition.

One caveat is that IntelliJ's profiler listens by default on port 10001, which conflicts with Accumulo's
[default master replication service port](https://accumulo.apache.org/1.8/accumulo_user_manual.html#_network).
You can fix this by manually setting the port in the `bin/idea.sh` file in your IntelliJ installation
according to the [instructions posted here](https://stackoverflow.com/questions/13345986/intellij-idea-using-10001-port).
Otherwise if you don't fix it, you may not be able to run IntelliJ and Accumulo concurrently.
You could also change Accumulo's ports from their default.



### Examples
The classes in [`src/test/kotlin/edu/washington/cs/laragraphulo/examples`](src/test/kotlin/edu/washington/cs/laragraphulo/examples)
contain simple, well-commented examples of how to use LaraDB.
To run an example, use the command `mvn test -Dtest=TESTNAME`, replacing `TESTNAME` with the name of the test.
To run every example, use the command `mvn test -Dtest=*Example`.

You may find it useful to run the examples on a standalone Accumulo instance,
using the instructions in the Test section above.
This has the advantage of retaining input and result tables 
in Accumulo, so that you may inspect them more closely after the example finishes.

The examples whose name ends with the suffix `_Graphulo_Example` run on Accumulo via calls to the Graphulo library;
examples with suffix `_Lara_Standalone_Example` run as an in-memory standalone program via the Lara API;
examples with suffix `_Lara_Accumulo_Example` run on an Accumulo via the Lara API.

#### RainySunny Example (Graphulo)
See the query expressed in the Lara API in 
[RainySunnyQuery](src/main/kotlin/edu/washington/cs/laragraphulo/examples/rainysunny/RainySunnyQuery.kt).
This query performs a simple map operation that changes all instances of the word "Rainy" to "Sunny" in a set of documents.

See the [rainy-sunny examples here](src/test/kotlin/edu/washington/cs/laragraphulo/examples/rainysunny).

#### WordCount Example (Lara Standalone)
See the query expressed in the Lara API in 
[WordCountQuery](src/main/kotlin/edu/washington/cs/laragraphulo/examples/wordcount/WordCountQuery.kt).
This example demonstrates the Lara API for the task of counting words across a collection of documents.

See the [word count examples here](src/test/kotlin/edu/washington/cs/laragraphulo/examples/wordcount).

#### Sensor Query Examples (Graphulo, Lara Standalone, Lara Accumulo)
See the query expressed in the Lara API in 
[SensorQuery](src/main/kotlin/edu/washington/cs/laragraphulo/examples/sensor/SensorQuery.kt).
This family of examples solves a sensory query task to compute the covariances of the differences in measurements 
between two sensors. The sensor data is read from [Array of Things](https://arrayofthings.github.io/) CSV files.

See the [sensor examples here](src/test/kotlin/edu/washington/cs/laragraphulo/examples/sensor).


### How to use LaraDB in Java client code
Include LaraDB's JAR in the Java classpath when running client code.  
This is automatically done if 

1. your client project is a maven project,
2. you first run `mvn install` to install LaraDB in your local maven repository,
3. and you add the following to your client project's pom.xml:
```
<dependency>
  <groupId>edu.washington.cs</groupId>
  <artifactId>lara-graphulo</artifactId>
  <version>${version}</version>
</dependency>
```

The following code snippet is a good starting point for using LaraDB:

```java
// setup
Instance instance = new ZooKeeperInstance(INSTANCE_NAME, INSTANCE_ZK_HOST);
Connector connector = instance.getConnector(USERNAME, PASSWORD_TOKEN);
Graphulo graphulo = new Graphulo(connector, PASSWORD_TOKEN);

// call LaraDB functions...
// TODO
```

See Examples above for more elaborate client code usage.


### Debug
Before debugging a problem, consider 

1. checking the Accumulo monitor, running by default on <http://localhost:9995/> for Accumulo 1.8;
2. printf or log4j debugging;
3. debugging by deletion, i.e., if removing a piece of code solves your problem, then you know where the problem is;
4. inserting the Accumulo system `DebugIterator` or the Graphulo `DebugInfoIterator` into the iterator stack,
which log information about every call from the iterator above it at the `DEBUG` or `INFO` level respectively.

Debuggers can be extraordinarily helpful once running but challenging to set up.
There are two methods to use a debugger: 

1. start a "debug server" in your IDE
and have a Java application connect to it at startup, or 
2. have a Java application start a "debug server" 
and listen for you to make a connection from your IDE.
 
#### Debugging Standalone Accumulo
I tend to use method (1) for standalone Accumulo.
Run the following before launching Accumulo via its start scripts or via `accumulo tserver &`:

```bash
export ACCUMULO_TSERVER_OPTS="-agentlib:jdwp=transport=dt_socket,server=n,address=127.0.0.1:5005,suspend=y" 
```

This will instruct the JVM for the Accumulo tablet server to connect to your IDE,
which you should configure to listen on port 5005 (of 127.0.0.1).

You must debug quickly in this mode.  If you idle too long without making a debug step,
then the Accumulo tablet server will lose its Zookeeper lock and kill itself.
You can increase the Zookeeper timeout in Accumulo's site config file.

#### Debugging MiniAccumulo
Set `-DTEST_CONFIG=miniDebug` for any test using MiniAccumulo.
The code will print to `System.out` the ports that MiniAccumulo randomly chooses to listen on 
for each Accumulo process after starting MiniAccumulo.
You have 10 seconds to connect to one of the ports (probably the TABLET_SERVER port)
before the test continues executing.
I recommend using `tail -f shippable/testresults/*TESTNAME-output.txt`
to see the ports as they are printed.  Replace `TESTNAME` with the name of the test you are running.

For a bit more insight, see the `before()` method of [MiniAccumuloTester][].

[MiniAccumuloTester]: src/test/kotlin/edu/washington/cs/laragraphulo/MiniAccumuloTester.java


## Unsorted

### [Array of Things](https://arrayofthings.github.io/) sensor data 
Code to ingest sensor data into Accumulo.
Query to compute the means and covariances of sensor classes.

### Cybersecurity data and RACO queries

Used as part of this polystore Jupyter notebook: 
<https://github.com/uwescience/raco/blob/SPJA_federation/HPDA_review.ipynb>
