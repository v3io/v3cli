## v3ctl

v3io command line utility

### General usage

v3ctl [command] [data-container] [path] [flags]


### Examples

```
   v3ctl ls                                         # List data containers (buckets)
   v3ctl ls datalake docs                           # List objects in docs directory at "datalake" data container
   echo "test" | v3ctl put datalake docs/test.txt   # Put/Upload object
   v3ctl getitems datalake mytable -a "*" -q "age>30"   # list records with selected fields and query
```

### Commands

```
  bash         init bash auto-completion, usage: source <(v3ctl bash)
  createstream Create a new stream with N shards
  del          Delete object
  delitems     Delete multiple records with optional filter
  get          Retrive object content
  getdir       Retrive object directory content
  getitem      Retrive record content/fields (as json struct)
  getitems     Retrive multiple records and fields (as json struct) based on query
  getrecords   Retrive one or more stream records
  help         Help about any command
  inferschema  Retrive multiple records and build schema file from the data
  ingest       Load data from file to stream or kv
  ls           List objects and directories (prefixes)
  put          Upload object content from input file or stdin
  putitem      Upload record content/fields from json input file or stdin
  putrecord    Upload stream record/message content from input file or stdin
  updateitem   update record content/fields using an expression (and optional condition)
```

### Global Options (for command specific options type v3cli [cmd] -h)

```
  -g, --config string                Path to a YAML configuration file. When this flag isn't
                                     set, the CLI checks for a v3io.yaml configuration file in the
                                     current directory. CLI flags override file cconfiguration
                                     Example: "~/cfg/my_v3io_cfg.yaml".
  -h, --help                         help for v3ctl
  -v, --log-level string[="debug"]   Verbose output. You can provide one of the following logging
                                     levels as an argument for this flag by using the assignment
                                     operator ('='): "debug" | "info" | "warn" | "error".
                                     For example: -v=info. The default log level when using this
                                     flag without an argument is "debug".
  -p, --password string              Password of the configured user (see -u|--username).
  -s, --server string                Web-gateway (web-APIs) service endpoint of an instance of
                                     the Iguazio Continuous Data Platfrom, of the format
                                     "<IP address>:<port number=8081>". Examples: "localhost:8081"
                                     (when running on the target platform); "192.168.1.100:8081".
  -u, --username string              Username of an Iguazio Continous Data Platform user.

Use "v3ctl [command] --help" for more information about a command.

```

