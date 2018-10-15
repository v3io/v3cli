package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strconv"
	"strings"
)

const bash_comp = `_v3cli() {
  COMPREPLY=()
  local word="${COMP_WORDS[COMP_CWORD]}"
  local completions=$(v3cli complete ${COMP_CWORD} "${COMP_WORDS[@]}")
  COMPREPLY=( $(compgen -W "$completions" -- "$word") )
}

complete -F _v3cli v3cli`

// for Bash auto completion
func NewCmdComplete(root *RootCommandeer) *cobra.Command {
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

			if err := root.initialize(); err != nil {
				os.Exit(1)
			}

			switch cword {
			case 1:
				fmt.Printf("%s", strings.Join(commands, " "))
			case 2:
				bkts, err := listAll(root)
				if err != nil {
					os.Exit(1)
				}
				list := []string{}
				for _, val := range bkts {
					list = append(list, val.Name)
				}
				fmt.Printf("%s", strings.Join(list, " "))
			case 3:
				if len(args) < 5 {
					os.Exit(1)
				}

				root.container = args[3]
				prefix := ""
				sp := strings.LastIndex(args[4], "/")
				if sp >= 0 {
					prefix = args[4][:sp]
				}

				resp, err := listBucket(root, prefix)
				if err != nil {
					os.Exit(1)
				}

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
		Use:   "bash",
		Short: "init bash auto-completion, usage: source <(v3cli bash)",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(bash_comp)
		},
	}
	return cmd
}
