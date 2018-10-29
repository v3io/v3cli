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

	input := v3io.GetItemsInput{Path: endWithSlash(c.rootCommandeer.dirPath), Filter: c.filter, AttributeNames: c.attributes}
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
	condition      string
}

func NewCmdPutitem(rootCommandeer *RootCommandeer) *putItemCommandeer {

	commandeer := &putItemCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "putitem [container-name] [table-path/key]",
		Short:   "Upload record content/fields from json input file or stdin",
		Aliases: []string{"pi"},
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

			return container.Sync.PutItem(&v3io.PutItemInput{
				Path: root.dirPath, Attributes: list, Condition: commandeer.condition})
		},
	}
	cmd.Flags().StringVarP(&rootCommandeer.inFile, "input-file", "f", "", "Input file for the different put* commands")
	cmd.Flags().StringVarP(&commandeer.condition, "condition", "n", "", "Update condition, update only if the condition is met")

	commandeer.cmd = cmd
	return commandeer
}

type updateItemCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	expression     string
	condition      string
}

func NewCmdUpdateItem(rootCommandeer *RootCommandeer) *updateItemCommandeer {

	commandeer := &updateItemCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "updateitem [container-name] [table-path/key]",
		Short:   "update record content/fields using an expression (and optional condition)",
		Aliases: []string{"ui"},
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			//TODO: add condition (to v3io-http)
			return container.Sync.UpdateItem(&v3io.UpdateItemInput{
				Path: root.dirPath, Expression: &commandeer.expression, Condition: commandeer.condition})
		},
	}

	cmd.Flags().StringVarP(&commandeer.expression, "expression", "e", "", "Update expression, e.g. x=5;y='good';z=z+1")
	cmd.Flags().StringVarP(&commandeer.condition, "condition", "n", "", "Update condition, update only if the condition is met")

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

type delItemsCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	filter         string
	force          bool
}

func NewCmdDelitems(rootCommandeer *RootCommandeer) *delItemsCommandeer {

	commandeer := &delItemsCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "delitems [container-name] [table-path] [-a attrs] [-q query]",
		Short:   "Delete multiple records with optional filter",
		Aliases: []string{"gis"},
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			if !commandeer.force {
				confirmedByUser, err := getConfirmation(
					fmt.Sprintf("You are about to delete the table '%s' in container '%s'. Are you sure?", root.dirPath, root.container))
				if err != nil {
					return err
				}

				if !confirmedByUser {
					return fmt.Errorf("Delete cancelled by the user.")
				}
			}

			return utils.DeleteTable(root.logger, container, endWithSlash(root.dirPath), commandeer.filter, root.v3iocfg.Workers)
		},
	}

	cmd.Flags().StringVarP(&commandeer.filter, "filter", "q", "", "GetItems query filter string, see getitems help for more")
	cmd.Flags().BoolVarP(&commandeer.force, "force", "f", false,
		"Forceful deletion - don't display a delete-verification prompt.")

	commandeer.cmd = cmd

	return commandeer
}
