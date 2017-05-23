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
	"encoding/json"
	"fmt"
	"github.com/iguazio/v3io"
	"io"
	"io/ioutil"
	"net/http"
	"time"
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

func RunPutItem(out io.Writer, url, container, path string, infile io.Reader, verbose bool) error {
	v3 := v3io.V3iow{"http://" + url + "/" + container, &http.Transport{}, verbose}
	bytes, err := ioutil.ReadAll(infile)
	if err != nil {
		return fmt.Errorf("Error reading input file (%v)\n", err)
	}
	list := make(map[string]interface{})
	err = json.Unmarshal(bytes, &list)
	resp, err := v3.UpdateItem(path, list)
	if err != nil {
		return fmt.Errorf("Error in Put/UpdateItem operation (%v)\n", err)
	}
	fmt.Fprintf(out, "%s\n", resp)
	return nil
}

func RunGetItem(out io.Writer, url, container, path, attrs string, verbose bool) error {
	v3 := v3io.V3iow{"http://" + url + "/" + container, &http.Transport{}, verbose}
	resp, err := v3.GetItem(path, attrs)
	if err != nil {
		return fmt.Errorf("Error in GetItem operation (%v)\n", err)
	}
	body, err := json.Marshal(resp.Item)
	if err != nil {
		return fmt.Errorf("Error in converting responce to Json (%v)\n", err)
	}
	fmt.Fprintf(out, "%s\n", body)
	return nil
}

func RunGetItems(out io.Writer, url, container, path, attrs, filter string, maxrec int, verbose bool) error {
	v3 := v3io.V3iow{"http://" + url + "/" + container, &http.Transport{}, verbose}
	marker := ""
	fmt.Fprintf(out, "[\n")
	first := true
	for {
		resp, err := v3.GetItems(path, attrs, filter, marker, maxrec, 0, 1)
		if err != nil {
			return fmt.Errorf("Error in GetItems operation (%v)\n", err)
		}
		for _, it := range resp.Items {
			body, err := json.Marshal(it)
			if err != nil {
				panic(err)
			}
			if !first {
				fmt.Fprintf(out, ",\n")
			}
			first = false
			fmt.Fprintf(out, "%s", body)
		}
		if resp.LastItemIncluded == "TRUE" {
			break
		}
		marker = resp.NextMarker
	}
	fmt.Fprintf(out, "\n]\n")
	return nil
}

func RunGetRecords(out io.Writer, url, container, path, stype string, maxrec, interval int, verbose bool) error {
	v3 := v3io.V3iow{"http://" + url + "/" + container, &http.Transport{}, verbose}
	v, err := v3.SeekShard(path, stype, 0)
	if err != nil {
		return fmt.Errorf("Error in Seek operation (%v)\n", err)
	}
	fmt.Fprintf(out, "Seek: %d\n", v)
	for {
		res3, err := v3.GetRecords(path, v, maxrec, 0)
		if err != nil {
			return fmt.Errorf("Error in GetRecords operation (%v)\n", err)
		}
		for _, d := range res3.Records {
			fmt.Fprintf(out, "%s\n", d.Data)
		}
		if interval == 0 {
			return nil
		}
		time.Sleep(time.Duration(interval) * time.Second)
		v = res3.NextLocation
	}
	return nil
}

func RunPutRecord(out io.Writer, url, container, path string, infile io.Reader, verbose bool) error {
	v3 := v3io.V3iow{"http://" + url + "/" + container, &http.Transport{}, verbose}
	bytes, err := ioutil.ReadAll(infile)
	if err != nil {
		return fmt.Errorf("Error reading input file (%v)\n", err)
	}
	msg := []string{string(bytes)}
	res2, err := v3.PutRecords(path, msg)
	if err != nil {
		return fmt.Errorf("Error in PutRecord operation (%v)\n", err)
	}
	fmt.Fprintf(out, "Resp: %s\n", res2)
	return nil
}

func RunCreateStream(out io.Writer, url, container, path string, shards, size int, verbose bool) error {
	v3 := v3io.V3iow{"http://" + url + "/" + container, &http.Transport{}, verbose}
	res, err := v3.CreateStream(path, shards, size)
	if err != nil {
		return fmt.Errorf("Error in CreateStream operation (%v)\n", err)
	}
	fmt.Fprintf(out, "Resp: %s\n", res)
	return nil
}
