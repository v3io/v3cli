package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-go-http"
	"os"
)

type lsCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	prefix         bool
	recursive      bool
	maxobj         int
}

func NewCmdLS(rootCommandeer *RootCommandeer) *lsCommandeer {

	commandeer := &lsCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:        "ls [container-name] [path]",
		Short:      "List objects and directories (prefixes)",
		Long:       GetLongHelp("ls"),
		Example:    GetExample("ls"),
		SuggestFor: []string{"list", "show"},
		RunE: func(cmd *cobra.Command, args []string) error {
			// select between list containers vs list directory/path
			return commandeer.list()
		},
	}

	cmd.Flags().BoolP("prefix", "x", false, "Show prefixes (directories) only")
	cmd.Flags().BoolP("recursive", "r", false, "Traverse tree recursively")
	cmd.Flags().IntP("max-obj", "m", 200, "Max objects to retrive")

	commandeer.cmd = cmd

	return commandeer
}

func (c *lsCommandeer) list() error {
	if err := c.rootCommandeer.initialize(); err != nil {
		return err
	}

	container, err := c.rootCommandeer.initV3io()
	if err != nil {
		return err
	}

	resp, err := container.Sync.ListBucket(&v3io.ListBucketInput{Path: c.rootCommandeer.dirPath})
	if err != nil {
		return err
	}

	result := resp.Output.(*v3io.ListBucketOutput)

	out := os.Stdout
	for _, val := range result.CommonPrefixes {
		fmt.Fprintf(out, "%s\n", val.Prefix)
	}
	fmt.Fprintf(out, "  SIZE     MODIFIED                 NAME\n")
	for _, val := range result.Contents {
		fmt.Fprintf(out, "%9d  %s  %s\n", val.Size, val.LastModified, val.Key)
	}
	return nil

}
