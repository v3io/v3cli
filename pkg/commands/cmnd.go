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
	"github.com/spf13/cobra"
	"github.com/v3io/v3cli/sdk"
)

func NewCmdGet() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get [container-name] [path]",
		Short:   "Retrive object content",
		Long:    GetLongHelp(""),
		Example: GetExample(""),
		RunE: func(cmd *cobra.Command, args []string) error {
			return sdk.RunGet(cmd.OutOrStdout(), Url, Container, Path, Verbose)
		},
	}

	AddWatch(cmd)
	return cmd
}

func NewCmdPut() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "put [container-name] [path]",
		Short:   "Upload object content from input file or stdin",
		Long:    GetLongHelp(""),
		Example: GetExample(""),
		RunE: func(cmd *cobra.Command, args []string) error {
			return sdk.RunPut(cmd.OutOrStdout(), Url, Container, Path, InFile, Verbose)
		},
	}
	AddInFile(cmd)
	return cmd
}
