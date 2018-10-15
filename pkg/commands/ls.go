package commands

import (
	"encoding/xml"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-go-http"
	"io/ioutil"
	"net/http"
)

type lsCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	prefix         bool
	recursive      bool
	maxobj         int
}

func NewCmdLS(rootCommandeer *RootCommandeer) *lsCommandeer {

	commandeer := &lsCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:        "ls [container-name] [path]",
		Short:      "List objects and directories (prefixes)",
		Long:       GetLongHelp("ls"),
		Example:    GetExample("ls"),
		SuggestFor: []string{"list", "show"},
		RunE: func(cmd *cobra.Command, args []string) error {
			// select between list containers vs list directory/path
			return commandeer.list()
		},
	}

	cmd.Flags().BoolP("prefix", "x", false, "Show prefixes (directories) only")
	cmd.Flags().BoolP("recursive", "r", false, "Traverse tree recursively")
	cmd.Flags().IntP("max-obj", "m", 200, "Max objects to retrive")

	commandeer.cmd = cmd

	return commandeer
}

func (c *lsCommandeer) list() error {

	root := c.rootCommandeer
	if err := root.initialize(); err != nil {
		return err
	}

	if root.container == "" {
		buckets, err := listAll(root)
		if err != nil {
			return err
		}

		for _, bucket := range buckets {
			fmt.Fprintf(root.out, "%6d  %-15s  %s\n", bucket.Id, bucket.Name, bucket.CreationDate)
		}

		return nil
	}

	container, err := root.initV3io()
	if err != nil {
		return err
	}

	resp, err := container.Sync.ListBucket(&v3io.ListBucketInput{Path: root.dirPath})
	if err != nil {
		return err
	}

	result := resp.Output.(*v3io.ListBucketOutput)

	for _, val := range result.CommonPrefixes {
		fmt.Fprintf(root.out, "%s\n", val.Prefix)
	}
	fmt.Fprintf(root.out, "  SIZE     MODIFIED                 NAME\n")
	for _, val := range result.Contents {
		fmt.Fprintf(root.out, "%9d  %s  %s\n", val.Size, val.LastModified, val.Key)
	}
	return nil

}

func listBucket(rc *RootCommandeer, prefix string) (*v3io.ListBucketOutput, error) {

	container, err := rc.initV3io()
	if err != nil {
		return nil, err
	}

	resp, err := container.Sync.ListBucket(&v3io.ListBucketInput{Path: prefix})
	if err != nil {
		return nil, err
	}

	return resp.Output.(*v3io.ListBucketOutput), nil
}

func listAll(rc *RootCommandeer) ([]v3io.Bucket, error) {

	if rc.v3iocfg.WebApiEndpoint == "" {
		return nil, fmt.Errorf("Error please specify API URL through the command line or config file")
	}

	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://"+rc.v3iocfg.WebApiEndpoint, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(rc.v3iocfg.Username, rc.v3iocfg.Password)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	output := v3io.ListAllOutput{}
	err = xml.Unmarshal(bodyText, &output)
	if err != nil {
		return nil, err
	}

	return output.Buckets.Bucket, nil
}
