Phrase Count
============

[![Build Status](https://travis-ci.org/fluo-io/phrasecount.svg?branch=master)](https://travis-ci.org/fluo-io/phrasecount)

An example application that computes phrase counts for unique documents using
Fluo. Each new unique document that is added causes phrase counts to be
incremented. Unique documents have reference counts based on the number of
locations that point to them.  When a unique document is no longer referenced
by any location, then the phrase counts will be decremented appropriately.  

After phrase counts are incremented, export transactions send phrase counts to
an Accumulo table.  The purpose of exporting data is to make it available for
query.  Percolator is not designed to support queries, because its transactions
are designed for throughput and not responsiveness.

Schema
------

This example uses the following schema. 
  
Row                   | Column                  | Value             | Purpose
----------------------|-------------------------|-------------------|---------------------------------------------------------------------
uri:&lt;uri&gt;       | doc:hash                | &lt;hash&gt;      | Contains the hash of the document found at the URI
doc:&lt;hash&gt;      | doc:content             | &lt;document&gt;  | The contents of the document
doc:&lt;hash&gt;      | doc:refCount            | &lt;int&gt;       | The number of URIs that reference this document 
doc:&lt;hash&gt;      | index:check             | empty             | Setting this columns triggers the observer that indexes the document 
doc:&lt;hash&gt;      | index:status            | INDEXED or empty  | Used to track the status of whether this document was indexed 
phrase:&lt;phrase&gt; | stat:check              | empty             | Triggers observer that handles high cardinality phrases
phrase:&lt;phrase&gt; | stat:docCount           | &lt;int&gt;       | Total number of documents the phrase occurred in
phrase:&lt;phrase&gt; | stat:sum                | &lt;int&gt;       | Total number of times the phrase was seen in all documents
phrase:&lt;phrase&gt; | stat:docCount&lt;int&gt;| &lt;int&gt;       | Random document count column used for high cardinality phrases
phrase:&lt;phrase&gt; | stat:sum&lt;int&gt;     | &lt;int&gt;       | Random count column used for high cardinality phrases
phrase:&lt;phrase&gt; | export:check            | empty             | Triggers export observer
phrase:&lt;phrase&gt; | export:docCount         | &lt;int&gt;       | Phrase docCount queued for export
phrase:&lt;phrase&gt; | export:seq              | &lt;int&gt;       | A sequence number used to order exports, as they may arrive out of order.
phrase:&lt;phrase&gt; | export:sum              | &lt;int&gt;       | Phrase sum queued for export

Code Overview
-------------

Documents are loaded into the Fluo table by [DocumentLoader][1] which is
executed by [Load][2].  [DocumentLoader][1] handles reference counting of
unique documents and may set a notification causing [PhraseCounter][3] to
execute.  [PhraseCounter][3] increments or decrements global phrase counts for
all phrases found in a unique document.  [PhraseCounter][3] is run by the
Fluo worker process and is configured by [Mini][4] when using java to run
this example.  [PhraseCounter][3] may set a notification which causes
[PhraseExporter][5] to run.  [PhraseExporter][5] exports phrases to a file with
a sequence number.  The sequence number allows you to know which version of the
phrase in the file is the most recent.  [PhraseExporter][5] can be configured
to export to an Accumulo table.

For high cardinality phrases, [PhraseCounter][3] will update a random column
and set a notification that causes [HCCounter][6] to run.  [HCCounter][6] will
read all of random columns and update the main count.  This breaks updating
high cardinality phrases into two transactions, as mentioned in the Percolator
paper.

Building
--------

After cloning this repository, build with following command. 
 
```
mvn package 
```

Running Mini Instance
---------------------

If you do not have Accumulo, Hadoop, Zookeeper, and Fluo setup, then you
can start an MiniFluo instance with the following command.  This command
will create an `fluo.properties` that can be used by the following commands
in this example.

```
mvn exec:java -Dexec.mainClass=phrasecount.cmd.Mini -Dexec.args="/tmp/mac fluo.properties" -Dexec.classpathScope=test 
```

After the mini command prints out `Wrote : fluo.properties` then its ready to use. 

This command will automatically configure [PhraseExporter][5] to export phrases
to an Accumulo table named `dataExport`.

The reason `-Dexec.classpathScope=test` is set is because it adds the test
[log4j.properties][7] file to the classpath.

Adding documents
----------------

The following command will scan the directory `$TXT_DIR` looking for .txt files to add.  The scan is recursive.  

```
mvn exec:java -Dexec.mainClass=phrasecount.cmd.Load -Dexec.args="fluo.properties $TXT_DIR" -Dexec.classpathScope=test
```

Printing phrases
----------------

After documents are added, the following command will print out phrase counts.
Try modifying a document you added and running the load command again, you
should eventually see the phrase counts change.

```
mvn exec:java -Dexec.mainClass=phrasecount.cmd.Print -Dexec.args="fluo.properties" -Dexec.classpathScope=test
```

The command will print out the number of unique documents and the number
of processed documents.  If the number of processed documents is less than the
number of unique documents, then there is still work to do.  After the load
command runs, the documents will have been added or updated.  However the
phrase counts will not update until the Observer runs in the background. 

Comparing exported phrases
--------------------------

After all export transactions have run, the phrase counts in the Accumulo
export table should be the same as those stored in the Fluo table.  The
following utility will iterate over the two and look for differences.

```
mvn exec:java -Dexec.mainClass=phrasecount.cmd.Compare -Dexec.args="fluo.properties data dataExport" -Dexec.classpathScope=test
```

If this command prints nothing, then all is good.  If things are not good, then
try enabling transaction trace logging and rerunning the scenario.  Adding the
following to log4j.properties will enable this tracing.  This configuration is
commented out in the test [log4j.properties][7] file.

```
log4j.logger.io.fluo.tx=TRACE
```

Deploying example
-----------------

The following instructions cover running this example on an installed Fluo
instance. Copy this jar to the Fluo observer directory.

```
cp target/phrasecount-0.0.1-SNAPSHOT.jar $FLUO_HOME/apps/<appname>/observers
```

Modify `$FLUO_HOME/apps/<appname>/fluo.properties` and replace the observer
lines with the following:

```
io.fluo.observer.0=phrasecount.PhraseCounter
io.fluo.observer.1=phrasecount.PhraseExporter,sink=accumulo,instance=${io.fluo.client.accumulo.instance},zookeepers=${io.fluo.client.accumulo.zookeepers},user=${io.fluo.client.accumulo.user},password=${io.fluo.client.accumulo.password},table=pcExport
io.fluo.observer.weak.0=phrasecount.HCCounter
```

The line with PhraseExporter has configuration options that need to be
configured to the Accumulo table where you want phrases to be exported.

Now initialize and start Fluo as outlined in its docs. Once started the
load and print commands above can be run passing in
`$FLUO_HOME/apps/<appname>/conf/fluo.properties`

There are two example bash scripts that run this test using the Fluo tar
distribution and serve as executable documentation for deployment.  The
previous maven commands using the exec plugin are convenient for a development
environment, using the following scripts shows how things would work in a
production environment.

  * [run-mini.sh](bin/run-mini.sh) : Runs this example using mini fluo started
    using the tar distribution.  Running this way does not require setting up
    Hadoop, Zookeeper, and Accumulo separately.  Just download or build the
    Fluo tar distribution, untar it, and point the script to that directory.
    This is the easiest way to simulate a production environment.
  * [run-cluster.sh] (bin/run-cluster.sh) : Runs this example with YARN using
    the Fluo tar distribution.  Running in this way requires setting up Hadoop,
    Zookeeper, and Accumulo instances separately.  The [fluo-dev][8] and
    [fluo-deploy][9] projects were created to ease setting up these external
    dependencies.

Generating data
---------------

Need some data? Use `elinks` to generate text files from web pages.

```
mkdir data
elinks -dump 1 -no-numbering -no-references http://accumulo.apache.org > data/accumulo.txt
elinks -dump 1 -no-numbering -no-references http://hadoop.apache.org > data/hadoop.txt
elinks -dump 1 -no-numbering -no-references http://zookeeper.apache.org > data/zookeeper.txt
```

[1]: src/main/java/phrasecount/DocumentLoader.java
[2]: src/main/java/phrasecount/cmd/Load.java
[3]: src/main/java/phrasecount/PhraseCounter.java
[4]: src/main/java/phrasecount/cmd/Mini.java
[5]: src/main/java/phrasecount/PhraseExporter.java
[6]: src/main/java/phrasecount/HCCounter.java
[7]: src/test/resources/log4j.properties
[8]: https://github.com/fluo-io/fluo-dev
[9]: https://github.com/fluo-io/fluo-deploy

