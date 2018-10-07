package commands

import (
	"github.com/spf13/cobra"
	"github.com/v3io/v3cli/help"
	"os"
)

var Verbose bool
var InFile *os.File
var Url, Container, Path string

func GetLongHelp(topic string) string {
	return help.LongHelp[topic] // TBD: use templates ..
}

func GetExample(topic string) string {
	return help.Examples[topic] // TBD: use templates ..
}

// Watch (blocking) flag option
func AddWatch(cmd *cobra.Command) {
	cmd.Flags().IntP("watch", "w", 0, "Watch object, read every N secounds (blocking)")
	cmd.Flags().Lookup("watch").NoOptDefVal = "2"
}

// input file flag option
func AddInFile(cmd *cobra.Command) {
	cmd.Flags().StringP("input-file", "f", "", "Input file for the different put* commands")
}
