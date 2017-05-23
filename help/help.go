/*
Copyright 2016 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package help

var LongHelp = map[string]string{
	"root": RootLong,
	"ls":   LSLong,
}

var Examples = map[string]string{
	"root": RootExamples,
	"ls":   LSExamples,
}

const RootLong string = `
v3io command line utility
`

const RootExamples string = `   v3cli ls                                         # List data containers (buckets)
   v3cli ls datalake docs                           # List objects in docs directory at "datalake" data container
   echo "test" | v3cli put datalake docs/test.txt   # Put/Upload object
   v3cli getitems datalake mytable -a "*" -q "age>30"   # list records with selected fields and query`

const LSLong string = `
List objects, files, tables, streams
`

const LSExamples string = `# List the data containers (buckets)
   v3cli ls

# list the objects in a data container
   v3cli ls datalake
   v3cli ls datalake /docs`
