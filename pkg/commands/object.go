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

package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-go-http"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
)

type getCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
}

func NewCmdGet(rootCommandeer *RootCommandeer) *getCommandeer {

	commandeer := &getCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "get [container-name] [path]",
		Short: "Retrive object content",
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			resp, err := container.Sync.GetObject(
				&v3io.GetObjectInput{Path: url.QueryEscape(commandeer.rootCommandeer.dirPath)})
			if err != nil {
				return fmt.Errorf("Error in GetObject operation (%v)", err)
			}
			fmt.Fprintf(root.out, string(resp.Body()))

			return nil
		},
	}

	commandeer.cmd = cmd
	return commandeer
}

type getDirCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	container      *v3io.Container
	suffix         string
	targetDir      string
	recursive      bool
	rootDir        string
}

func NewCmdDirGet(rootCommandeer *RootCommandeer) *getDirCommandeer {

	commandeer := &getDirCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "getdir [container-name] [path]",
		Short: "Retrive object directory content",
		RunE: func(cmd *cobra.Command, args []string) error {

			if err := rootCommandeer.initialize(); err != nil {
				return err
			}

			var err error
			commandeer.container, err = rootCommandeer.initV3io()
			if err != nil {
				return err
			}

			if rootCommandeer.dirPath != "" {
				commandeer.rootDir = endWithSlash(rootCommandeer.dirPath)
			}

			if commandeer.targetDir != "" {
				commandeer.targetDir = endWithSlash(commandeer.targetDir)
				CreateDirIfNotExist(commandeer.targetDir)
			}

			return commandeer.getDir(commandeer.rootDir)
		},
	}
	cmd.Flags().StringVarP(&commandeer.targetDir, "target-dir", "t", "", "Target directory for files")
	cmd.Flags().StringVarP(&commandeer.suffix, "suffix", "e", "*", "Suffix filter e.g. *.png")
	cmd.Flags().BoolVarP(&commandeer.recursive, "recursive", "r", false, "recursive")

	commandeer.cmd = cmd
	return commandeer
}

func (c *getDirCommandeer) getDir(path string) error {

	resp, err := c.container.Sync.ListBucket(&v3io.ListBucketInput{Path: path})
	if err != nil {
		return err
	}
	result := resp.Output.(*v3io.ListBucketOutput)

	for _, val := range result.Contents {

		match, err := filepath.Match(c.rootDir+c.suffix, val.Key)
		if err != nil {
			return err
		}
		if match {
			resp, err = c.container.Sync.GetObject(&v3io.GetObjectInput{Path: url.QueryEscape(val.Key)})
			if err != nil {
				return fmt.Errorf("Error in GetObject operation (%v)", err)
			}

			err = writeFile(c.targetDir+val.Key[len(c.rootDir):], resp.Body())
			if err != nil {
				return err
			}
		}

	}

	if c.recursive {
		for _, val := range result.CommonPrefixes {
			CreateDirIfNotExist(c.targetDir + val.Prefix[len(c.rootDir):])
			c.getDir(val.Prefix)
		}
	}

	return nil
}

func CreateDirIfNotExist(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			panic(err)
		}
	}
}

func writeFile(path string, bytes []byte) error {
	file, err := os.OpenFile(
		path,
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0666,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write bytes to file
	bytesWritten, err := file.Write(bytes)
	if err != nil {
		return err
	}
	fmt.Printf("Wrote %s (%d bytes).\n", path, bytesWritten)

	return nil
}

type putCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
}

func NewCmdPut(rootCommandeer *RootCommandeer) *putCommandeer {

	commandeer := &putCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "put [container-name] [path]",
		Short: "Upload object content from input file or stdin",
		RunE: func(cmd *cobra.Command, args []string) error {

			bytes, err := ioutil.ReadAll(commandeer.rootCommandeer.in)
			if err != nil {
				return fmt.Errorf("Error reading input file (%v)\n", err)
			}

			root := commandeer.rootCommandeer
			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			return container.Sync.PutObject(&v3io.PutObjectInput{Path: url.QueryEscape(root.dirPath), Body: bytes})
		},
	}
	cmd.Flags().StringVarP(&rootCommandeer.inFile, "input-file", "f", "", "Input file for the different put* commands")

	commandeer.cmd = cmd
	return commandeer
}

type delCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
}

func NewCmdDel(rootCommandeer *RootCommandeer) *delCommandeer {

	commandeer := &delCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "del [container-name] [path]",
		Short: "Delete object",
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			return container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: url.QueryEscape(root.dirPath)})
		},
	}

	commandeer.cmd = cmd
	return commandeer
}
