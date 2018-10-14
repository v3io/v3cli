package commands

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/v3io/v3cli/pkg/utils"
	"github.com/v3io/v3io-go-http"
	"io/ioutil"
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

type putItemCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
}

func NewCmdPutitem(rootCommandeer *RootCommandeer) *putItemCommandeer {

	commandeer := &putItemCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "putitem [container-name] [table-path/key]",
		Short:   "Upload record content/fields from json input file or stdin",
		Long:    GetLongHelp(""),
		Example: GetExample(""),
		Aliases: []string{"puti"},
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

			list := make(map[string]interface{})
			err = json.Unmarshal(bytes, &list)
			if err != nil {
				return fmt.Errorf("failed to unmarshal results (%v)", err)
			}

			return container.Sync.PutItem(&v3io.PutItemInput{Path: root.dirPath, Attributes: list})
		},
	}
	cmd.Flags().StringVarP(&rootCommandeer.inFile, "input-file", "f", "", "Input file for the different put* commands")

	commandeer.cmd = cmd
	return commandeer
}

type getItemCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	attributes     []string
}

func NewCmdGetitem(rootCommandeer *RootCommandeer) *getItemCommandeer {

	commandeer := &getItemCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "getitem [container-name] [table-path/key]",
		Short:   "Retrive record content/fields (as json struct)",
		Long:    GetLongHelp(""),
		Example: GetExample(""),
		Aliases: []string{"gi"},
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			input := v3io.GetItemInput{Path: commandeer.rootCommandeer.dirPath, AttributeNames: commandeer.attributes}
			resp, err := container.Sync.GetItem(&input)
			if err != nil {
				return fmt.Errorf("Error in GetItem operation (%v)", err)
			}
			output := resp.Output.(*v3io.GetItemOutput)

			body, err := json.Marshal(output.Item)
			if err != nil {
				return err
			}
			fmt.Fprintf(commandeer.rootCommandeer.out, "%s", body)

			return nil
		},
	}
	cmd.Flags().StringSliceVarP(&commandeer.attributes, "attrs", "a", []string{"*"}, "GetItem(s) Columns to return seperated by ','")

	commandeer.cmd = cmd

	return commandeer
}
