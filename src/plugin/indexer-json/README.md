indexer-json plugin for Nutch
=============================

**indexer-json plugin** is used for writing documents to a JSONL file, one JSON document per line. The configuration for the index writers is on **conf/index-writers.xml** file, included in the official Nutch distribution and it's as follow:

```xml
<writer id="<writer_id>" class="org.apache.nutch.indexwriter.json.JsonIndexWriter">
  <mapping>
    ...
  </mapping>
  <parameters>
    ...
  </parameters>
</writer>
```

Each `<writer>` element has two mandatory attributes:

* `<writer_id>` is a unique identification for each configuration. This feature allows Nutch to distinguish each configuration, even when they are for the same index writer. In addition, it allows to have multiple instances for the same index writer, but with different configurations.

* `org.apache.nutch.indexwriter.json.JsonIndexWriter` corresponds to the canonical name of the class that implements the IndexWriter extension point. This value should not be modified for the **indexer-json plugin**.

## Mapping

The mapping section is explained [here](https://cwiki.apache.org/confluence/display/NUTCH/IndexWriters#IndexWriters-Mappingsection). The structure of this section is general for all index writers.

## Parameters

Each parameter has the form `<param name="<name>" value="<value>"/>` and the parameters for this index writer are:

Parameter Name | Description | Default value
--|--|--
outpath | Output path / directory (local filesystem path, relative to current working directory) | jsonindexwriter
