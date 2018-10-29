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
	"strings"
	"time"
)

type createStreamCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	shards         int
	size           int
	retention      int
}

func NewCmdCreatestream(rootCommandeer *RootCommandeer) *createStreamCommandeer {

	commandeer := &createStreamCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "createstream [container-name] [stream-path]",
		Short:   "Create a new stream with N shards",
		Aliases: []string{"cs"},
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			err = container.Sync.CreateStream(&v3io.CreateStreamInput{
				Path: endWithSlash(root.dirPath), ShardCount: commandeer.shards, RetentionPeriodHours: commandeer.retention})
			if err != nil {
				root.logger.ErrorWith("CreateStream failed", "path", endWithSlash(root.dirPath), "err", err)
			}

			return nil
		},
	}

	cmd.Flags().IntVarP(&commandeer.shards, "shards", "n", 1, "Number of shards (partitions)")
	cmd.Flags().IntVarP(&commandeer.size, "shardsize", "z", 10, "Stream shard size in MB")
	cmd.Flags().IntVarP(&commandeer.retention, "retention", "r", 24, "Stream retention time in hours")

	commandeer.cmd = cmd
	return commandeer
}

type getrecordCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	seek           string
	time           int
	maxrec         int
	sequence       int
	watch          int
}

func NewCmdGetrecord(rootCommandeer *RootCommandeer) *getrecordCommandeer {

	commandeer := &getrecordCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "getrecords [container-name] [stream-path/shard-id] [-s seek][-t time][-n seq][-m max][-w int]",
		Short:   "Retrive one or more stream records",
		Aliases: []string{"gr"},
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
			if root.dirPath == "" {
				return fmt.Errorf("missing stream path (<stream>/<shard-id>")
			}
			input := v3io.SeekShardInput{Path: root.dirPath}

			switch strings.ToLower(commandeer.seek) {
			case "time":
				input.Type = v3io.SeekShardInputTypeTime
				input.Timestamp = commandeer.time
			case "seq", "sequence":
				input.Type = v3io.SeekShardInputTypeSequence
				input.StartingSequenceNumber = commandeer.sequence
			case "latest", "late":
				input.Type = v3io.SeekShardInputTypeLatest
			case "earliest":
				input.Type = v3io.SeekShardInputTypeEarliest
			default:
				return fmt.Errorf(
					"Stream seek type %s is invalid, use time | seq | latest | earliest", commandeer.seek)

			}

			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			resp, err := container.Sync.SeekShard(&input)
			if err != nil {
				return fmt.Errorf("Error in Seek operation, make sure the path include the shard id (e.g. <stream>/0) - %v", err)
			}
			location := resp.Output.(*v3io.SeekShardOutput).Location
			fmt.Fprintln(root.out, "Seek location:", location)

			for {
				resp, err := container.Sync.GetRecords(&v3io.GetRecordsInput{
					Path:     root.dirPath,
					Location: location,
					Limit:    commandeer.maxrec,
				})

				if err != nil {
					return fmt.Errorf("Error in GetRecords operation (%v)", err)
				}
				output := resp.Output.(*v3io.GetRecordsOutput)
				for _, r := range output.Records {
					fmt.Fprintln(root.out, "Time:", time.Unix(int64(r.ArrivalTimeSec), int64(r.ArrivalTimeNSec)),
						"Seq:", r.SequenceNumber, "PartitionKey:", r.PartitionKey)
					if r.ClientInfo != nil {
						fmt.Fprintf(root.out, "ClientInfo: %s\nData:\n", string(r.ClientInfo))
					}
					fmt.Fprintf(root.out, "%s\n", string(r.Data))
				}

				location = output.NextLocation
				if output.RecordsBehindLatest == 0 {
					if commandeer.watch == 0 {
						return nil
					}
					time.Sleep(time.Duration(commandeer.watch) * time.Second)
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&commandeer.seek, "seek", "k", "EARLIEST", "Relative stream location [EARLIEST | LATEST | SEQUENCE | TIME]")
	cmd.Flags().IntVarP(&commandeer.time, "time", "t", 0, "Starting time - for TIME seek")
	cmd.Flags().IntVarP(&commandeer.sequence, "sequence", "n", 0, "Starting sequence - for SEQUENCE seek")
	cmd.Flags().IntVarP(&commandeer.maxrec, "max-rec", "m", 50, "Max Records/Items to get per call")
	cmd.Flags().IntVarP(&commandeer.watch, "watch", "w", 0, "Watch object, read every N secounds (blocking)")
	cmd.Flags().Lookup("watch").NoOptDefVal = "2"

	commandeer.cmd = cmd
	return commandeer
}

type putrecordCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	shardid        int
	partitionKey   string
	clientInfo     string
}

func NewCmdPutrecord(rootCommandeer *RootCommandeer) *putrecordCommandeer {

	commandeer := &putrecordCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "putrecord [container-name] [stream-path]",
		Short:   "Upload stream record/message content from input file or stdin",
		Aliases: []string{"pr"},
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

			records := []*v3io.StreamRecord{{
				Data: bytes, ClientInfo: []byte(commandeer.clientInfo), PartitionKey: commandeer.partitionKey,
			}}
			_, err = container.Sync.PutRecords(&v3io.PutRecordsInput{
				Path:    endWithSlash(root.dirPath),
				Records: records,
			})

			return err
		},
	}
	cmd.Flags().StringVarP(&rootCommandeer.inFile, "input-file", "f", "", "Input file for the different put* commands")
	cmd.Flags().StringVarP(&commandeer.partitionKey, "partition-key", "k", "", "Partition key (used to determine shard)")
	cmd.Flags().StringVarP(&commandeer.clientInfo, "client-info", "c", "", "ClientInfo, extra metadata for the message")

	commandeer.cmd = cmd
	return commandeer
}
