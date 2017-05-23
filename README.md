## v3cli

v3io command line utility

### General usage

v3cli [command] [container] [path] [flags]


### Examples

```
   v3cli ls                                         # List data containers (buckets)
   v3cli ls datalake docs                           # List objects in docs directory at "datalake" data container
   echo "test" | v3cli put datalake docs/test.txt   # Put/Upload object
   v3cli getitems datalake mytable -a "*" -q "age>30"   # list records with selected fields and query
```

### Global Options (for command specific options type v3cli <cmd> -h)

```
  -h, --help             help for v3cli
  -v, --verbose          Verbose output
  -u, --web_url string   Url to v3io web APIs, can be specified in V3IO_WEB_URL env var
```

### Commands
* [v3cli createstream]	 - Create a new stream with N shards
* [v3cli get]	 - Retrive object content
* [v3cli getitem]	 - Retrive record content/fields (as json struct)
* [v3cli getitems]	 - Retrive multiple records and fields (as json struct) based on query
* [v3cli getrecords]	 - Retrive one or more stream records
* [v3cli ls]	 - List objects and directories (prefixes)
* [v3cli put]	 - Upload object content from input file or stdin
* [v3cli putitem]	 - Upload record content/fields from json input file or stdin
* [v3cli putrecord]	 - Upload stream record/message content from input file or stdin

