package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-go-http"
	"io/ioutil"
)

type getCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
}

func NewCmdGet(rootCommandeer *RootCommandeer) *getCommandeer {

	commandeer := &getCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "get [container-name] [path]",
		Short: "Retrive object content",
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			resp, err := container.Sync.GetObject(&v3io.GetObjectInput{Path: commandeer.rootCommandeer.dirPath})
			if err != nil {
				return fmt.Errorf("Error in GetObject operation (%v)", err)
			}
			fmt.Fprintf(root.out, string(resp.Body()))

			return nil
		},
	}

	commandeer.cmd = cmd
	return commandeer
}

type putCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
}

func NewCmdPut(rootCommandeer *RootCommandeer) *putCommandeer {

	commandeer := &putCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "put [container-name] [path]",
		Short: "Upload object content from input file or stdin",
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

			return container.Sync.PutObject(&v3io.PutObjectInput{Path: root.dirPath, Body: bytes})
		},
	}
	cmd.Flags().StringVarP(&rootCommandeer.inFile, "input-file", "f", "", "Input file for the different put* commands")

	commandeer.cmd = cmd
	return commandeer
}

type delCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
}

func NewCmdDel(rootCommandeer *RootCommandeer) *delCommandeer {

	commandeer := &delCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "del [container-name] [path]",
		Short: "Delete object",
		RunE: func(cmd *cobra.Command, args []string) error {

			root := commandeer.rootCommandeer
			if err := root.initialize(); err != nil {
				return err
			}

			container, err := root.initV3io()
			if err != nil {
				return err
			}

			return container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: root.dirPath})
		},
	}

	commandeer.cmd = cmd
	return commandeer
}
