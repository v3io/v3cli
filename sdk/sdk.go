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
package sdk

import (
	"fmt"
	"github.com/iguazio/v3io"
	"io"
	"io/ioutil"
	"net/http"
)

// List all data containers
func ListAll(url string, verbose bool) (v3io.BckList, error) {
	v3 := v3io.V3iow{"http://" + url, &http.Transport{}, verbose}
	resp, err := v3.ListAll("http://" + url)
	if err != nil {
		return v3io.BckList{}, fmt.Errorf("Error in Listing Containers in Path %s (%v)\n", url, err)
	}
	return resp.Buckets, err
}

func RunLS(out io.Writer, url, container, path string, pfx, verbose bool) error {
	v3 := v3io.V3iow{"http://" + url + "/" + container, &http.Transport{}, verbose}
	resp, err := v3.ListBucket(path)
	if err != nil {
		return fmt.Errorf("Error in accessing container, check name (%v)\n", err)
	}
	for _, val := range resp.CommonPrefixes {
		fmt.Fprintf(out, "%s\n", val.Prefix)
	}
	fmt.Fprintf(out, "  SIZE     MODIFIED                 NAME\n")
	for _, val := range resp.Contents {
		fmt.Fprintf(out, "%9d  %s  %s\n", val.Size, val.LastModified, val.Key)
	}
	return nil
}

func RunGet(out io.Writer, url, container, path string, verbose bool) error {
	v3 := v3io.V3iow{"http://" + url + "/" + container, &http.Transport{}, verbose}
	resp, err := v3.Get(path)
	if err != nil {
		return fmt.Errorf("Error in Put operation (%v)\n", err)
	}
	fmt.Fprintf(out, "%s\n", resp)
	return nil
}

func RunPut(out io.Writer, url, container, path string, infile io.Reader, verbose bool) error {
	v3 := v3io.V3iow{"http://" + url + "/" + container, &http.Transport{}, verbose}
	bytes, err := ioutil.ReadAll(infile)
	if err != nil {
		return fmt.Errorf("Error reading input file (%v)\n", err)
	}
	resp, err := v3.Put(path, bytes)
	if err != nil {
		return fmt.Errorf("Error in Put operation (%v)\n", err)
	}
	fmt.Fprintf(out, "%s\n", resp)
	return nil
}
