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
	"github.com/v3io/frames"
	"github.com/v3io/v3cli/pkg/utils"
	"github.com/v3io/v3io-go-http"
)

type inferSchemaCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	keyField       string
	maxrec         int
}

func NewCmdInferSchema(rootCommandeer *RootCommandeer) *inferSchemaCommandeer {

	commandeer := &inferSchemaCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "inferschema [container-name] [table-path] [-k key] [-m max-rec]",
		Short:   "Retrive multiple records and build schema file from the data",
		Aliases: []string{"is"},
		RunE: func(cmd *cobra.Command, args []string) error {

			return commandeer.inferSchema()
		},
	}

	cmd.Flags().StringVarP(&commandeer.keyField, "key", "k", "__name", "name of the key column")
	cmd.Flags().IntVarP(&commandeer.maxrec, "max-rec", "m", 50, "Max Records/Items to get per call")

	commandeer.cmd = cmd

	return commandeer
}

func (c *inferSchemaCommandeer) inferSchema() error {

	if err := c.rootCommandeer.initialize(); err != nil {
		return err
	}

	container, err := c.rootCommandeer.initV3io()
	if err != nil {
		return err
	}

	input := v3io.GetItemsInput{
		Path: endWithSlash(c.rootCommandeer.dirPath), Filter: "", AttributeNames: []string{"*"}}
	c.rootCommandeer.logger.DebugWith("GetItems for schema", "input", input)
	iter, err := utils.NewAsyncItemsCursor(
		container, &input, c.rootCommandeer.v3iocfg.QryWorkers, []string{}, c.rootCommandeer.logger, 0)
	if err != nil {
		return err
	}

	out := c.rootCommandeer.out
	rowSet := []map[string]interface{}{}
	indicies := []string{}

	for rowNum := 0; rowNum < c.maxrec && iter.Next(); rowNum++ {
		row := iter.GetFields()
		rowSet = append(rowSet, row)
		index, ok := row["__name"]
		if !ok {
			return fmt.Errorf("key (__name) was not found in row")
		}
		indicies = append(indicies, index.(string))
	}

	if iter.Err() != nil {
		return iter.Err()
	}

	labels := map[string]interface{}{}
	frame, err := frames.NewFrameFromRows(rowSet, indicies, labels)
	if err != nil {
		return fmt.Errorf("Failed to create frame - %v", err)
	}

	nullSchema := utils.NewSchema(c.keyField)
	newSchema := utils.NewSchema(c.keyField)

	for _, name := range frame.Names() {
		col, err := frame.Column(name)
		if err != nil {
			return err
		}
		err = newSchema.AddColumn(name, col, true)
		if err != nil {
			return err
		}
	}

	bytes, _ := newSchema.ToJson()
	fmt.Fprintln(out, string(bytes))

	err = nullSchema.UpdateSchema(container, endWithSlash(c.rootCommandeer.dirPath), newSchema)
	if err != nil {
		return err
	}

	return nil
}
