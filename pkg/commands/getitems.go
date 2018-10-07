package commands

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/v3io/v3cli/pkg/utils"
	"github.com/v3io/v3io-go-http"
)

type getItemsCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	attributes     []string
	filter         string
	maxrec         int
}

func NewCmdGetitems(rootCommandeer *RootCommandeer) *getItemsCommandeer {

	commandeer := &getItemsCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "getitems [container-name] [table-path] [-a attrs] [-q query]",
		Short:   "Retrive multiple records and fields (as json struct) based on query",
		Long:    GetLongHelp(""),
		Example: GetExample(""),
		Aliases: []string{"gis"},
		RunE: func(cmd *cobra.Command, args []string) error {

			return commandeer.getitems()
			//attrs, _ := cmd.Flags().GetString("attrs")
			//query, _ := cmd.Flags().GetString("query")
			//maxrec, _ := cmd.Flags().GetInt("max-rec")
			//return sdk.RunGetItems(cmd.OutOrStdout(), Url, Container, Path, attrs, query, maxrec, Verbose)
		},
	}

	cmd.Flags().StringSliceVarP(&commandeer.attributes, "attrs", "a", []string{"*"}, "GetItem(s) Columns to return seperated by ','")
	cmd.Flags().StringVarP(&commandeer.filter, "filter", "q", "", "GetItems query filter string, see getitems help for more")
	cmd.Flags().IntVarP(&commandeer.maxrec, "max-rec", "m", 50, "Max Records/Items to get per call")

	commandeer.cmd = cmd

	return commandeer
}

func (c *getItemsCommandeer) getitems() error {

	if err := c.rootCommandeer.initialize(); err != nil {
		return err
	}

	container, err := c.rootCommandeer.initV3io()
	if err != nil {
		return err
	}

	input := v3io.GetItemsInput{Path: c.rootCommandeer.dirPath, Filter: c.filter, AttributeNames: c.attributes}
	c.rootCommandeer.logger.DebugWith("GetItems input", "input", input)
	iter, err := utils.NewAsyncItemsCursor(
		container, &input, c.rootCommandeer.v3iocfg.QryWorkers, []string{}, c.rootCommandeer.logger, 0)
	if err != nil {
		return err
	}

	out := c.rootCommandeer.out
	fmt.Fprintf(out, "[\n")
	first := true

	for rowNum := 0; rowNum < c.maxrec && iter.Next(); rowNum++ {
		row := iter.GetFields()
		body, err := json.Marshal(row)
		if err != nil {
			return err
		}
		if !first {
			fmt.Fprintf(out, ",\n")
		}
		first = false
		fmt.Fprintf(out, "%s", body)
	}
	fmt.Fprintf(out, "\n]\n")

	if iter.Err() != nil {
		return iter.Err()
	}

	return nil
}
