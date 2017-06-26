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
package main

import (
	"fmt"
	"strings"

	"github.com/iguazio/v3io"
	"github.com/spf13/cobra"
	"github.com/v3io/v3cli/cmnd"
	"github.com/v3io/v3cli/sdk"
	"net/http"
	"os"
	"strconv"
	"github.com/spf13/cobra/doc"
)

const bash_comp = `_v3cli() {
  COMPREPLY=()
  local word="${COMP_WORDS[COMP_CWORD]}"
  local completions=$(v3cli complete ${COMP_CWORD} "${COMP_WORDS[@]}")
  COMPREPLY=( $(compgen -W "$completions" -- "$word") )
}

complete -F _v3cli v3cli`


func main() {
	// define root CLI command
	var rootCmd = &cobra.Command{
		Use:     "v3cli [command] [container] [path] [flags]",
		Short:   "v3io command line utility",
		Long:    cmnd.GetLongHelp("root"),
		Example: cmnd.GetExample("root"),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			file, err := cmd.Flags().GetString("input-file")
			if err == nil {
				err = cmnd.GetInFile(file)
				if err != nil {
					return err
				}
			}
			if file == "" {
				cmnd.InFile = os.Stdin
			} else {
				cmnd.InFile, err = os.Open(file)
				if err != nil {
					return fmt.Errorf("Failed to open input file: %s\n", err)
				}
			}
			if len(args) > 0 {
				cmnd.Container = args[0]
			}
			if len(args) > 1 {
				cmnd.Path = args[1]
			}
			if (cmd.Name() != "ls" && cmd.Name() != "complete" && cmd.Name() != "bash") && len(args) < 1 {
				return fmt.Errorf("Please specify container Name/Id, Path and parameters !\n")
			}

			return nil
		},
	}

	// Global flags
	rootCmd.PersistentFlags().BoolVarP(&cmnd.Verbose, "verbose", "v", false, "Verbose output")
	rootCmd.PersistentFlags().StringVarP(&cmnd.Url, "web_url", "u", os.Getenv("V3IO_WEB_URL"),
		"Url to v3io web APIs, can be specified in V3IO_WEB_URL env var")

	// link child commands
	rootCmd.AddCommand(cmnd.NewCmdLS(), cmnd.NewCmdGet(), cmnd.NewCmdPut(), cmnd.NewCmdPutitem(), cmnd.NewCmdGetitem(),
		cmnd.NewCmdGetitems(), cmnd.NewCmdGetrecord(), cmnd.NewCmdPutrecord(), cmnd.NewCmdCreatestream(),
		NewCmdComplete(),NewCmdBash(), cmnd.NewCmdIngest())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Auto generate MD
func GenMarkdown(cmd *cobra.Command) {
	err := doc.GenMarkdownTree(cmd, "~/doc")
	if err != nil {
		panic(err)
	}
}

// for Bash auto completion
func NewCmdComplete() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "complete [path to complete]",
		Short:  "bash completion helper",
		Hidden: true,
		Run: func(cmd *cobra.Command, args []string) {
			cword, _ := strconv.Atoi(args[0])
			var commands []string
			for _, c := range cmd.Parent().Commands() {
				commands = append(commands, c.Name())
			}

			switch cword {
			case 1:
				fmt.Printf("%s", strings.Join(commands, " "))
			case 2:
				bkts, err := sdk.ListAll(cmnd.Url, cmnd.Verbose)
				if err != nil {
					os.Exit(1)
				}
				list := []string{}
				for _, val := range bkts.Bucket {
					list = append(list, fmt.Sprintf("%d", val.Id))
				}
				fmt.Printf("%s", strings.Join(list, " "))
			case 3:
				cn := args[3]
				v3 := v3io.V3iow{"http://" + cmnd.Url + "/" + cn, &http.Transport{}, false}
				prefix := ""
				sp := strings.LastIndex(args[4], "/")
				if sp >= 0 {
					prefix = args[4][:sp]
				}

				resp, _ := v3.ListBucket(prefix)
				list := []string{}
				for _, val := range resp.CommonPrefixes {
					list = append(list, val.Prefix)
				}
				for _, val := range resp.Contents {
					list = append(list, val.Key)
				}
				fmt.Printf("%s", strings.Join(list, " "))
			default:
				fmt.Println("")
			}

			os.Exit(0)
		},
	}
	return cmd
}
// for Bash auto completion, output bash init string
func NewCmdBash() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "bash",
		Short:  "init bash auto-completion, usage: source <(v3cli bash)",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(bash_comp)
		},
	}
	return cmd
}
