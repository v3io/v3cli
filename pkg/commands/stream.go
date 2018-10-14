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
		Long:    GetLongHelp(""),
		Example: GetExample(""),
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
				Path: root.dirPath, ShardCount: commandeer.shards, RetentionPeriodHours: commandeer.retention})
			if err != nil {
				root.logger.ErrorWith("CreateStream failed", "path", root.dirPath, "err", err)
			}

			return nil
		},
	}

	cmd.Flags().IntVarP(&commandeer.shards, "shards", "n", 1, "Number of shards (partitions)")
	cmd.Flags().IntVarP(&commandeer.size, "shardsize", "z", 10, "Stream shard size in MB")
	cmd.Flags().IntVarP(&commandeer.retention, "retention", "r", 7, "Stream retention time in days")

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
		Long:    GetLongHelp(""),
		Example: GetExample(""),
		Aliases: []string{"gr"},
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
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
				return fmt.Errorf("Error in Seek operation (%v)", err)
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
						"Seq:", r.SequenceNumber)
					fmt.Fprintf(root.out, "%s\n", string(r.Data))
				}
				if commandeer.watch == 0 {
					return nil
				}
				time.Sleep(time.Duration(commandeer.watch) * time.Second)
				location = output.NextLocation
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
}

func NewCmdPutrecord(rootCommandeer *RootCommandeer) *putrecordCommandeer {

	commandeer := &putrecordCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "putrecord [container-name] [stream-path]",
		Short:   "Upload stream record/message content from input file or stdin",
		Long:    GetLongHelp(""),
		Example: GetExample(""),
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

			records := []*v3io.StreamRecord{{Data: bytes}}
			_, err = container.Sync.PutRecords(&v3io.PutRecordsInput{
				Path: root.dirPath, Records: records})

			return err
		},
	}
	cmd.Flags().StringVarP(&rootCommandeer.inFile, "input-file", "f", "", "Input file for the different put* commands")

	commandeer.cmd = cmd
	return commandeer
}
