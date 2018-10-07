package commands

import (
	"fmt"
	"github.com/iguazio/v3io"
	"github.com/spf13/cobra"
	"github.com/v3io/v3cli/sdk"
	"net/http"
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
				bkts, err := sdk.ListAll(Url, Verbose)
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
				v3 := v3io.V3iow{"http://" + Url + "/" + cn, &http.Transport{}, false}
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
		Use:   "bash",
		Short: "init bash auto-completion, usage: source <(v3cli bash)",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(bash_comp)
		},
	}
	return cmd
}
