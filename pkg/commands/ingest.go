package commands

import (
	"github.com/spf13/cobra"
	"github.com/v3io/http_blaster/httpblaster"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/tui"
	"strings"
	"sync"
)

const IngestExample = `
./v3ctl ingest 1 test_emd5/ -u 192.168.206.10:8081  --generator csv2kv
 --payload-path ../http_blaster/examples/payloads/order-book-sample.csv
 --schema-path ../http_blaster/examples/schemas/schema_example.json -w 10`

func NewCmdIngest(rootCommandeer *RootCommandeer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ingest [container-name] [target-path] [url]",
		Short:   "Load data from file to stream or kv",
		Example: IngestExample,
		RunE: func(cmd *cobra.Command, args []string) error {

			if err := rootCommandeer.initialize(); err != nil {
				return err
			}

			url := rootCommandeer.v3ioPath

			var host string
			var port string
			url_arr := strings.Split(url, ":")
			if len(url_arr) == 1 {
				host = url
			} else {
				host = url_arr[0]
				port = url_arr[1]
			}
			payload, _ := cmd.Flags().GetString("payload-path")
			schema_path, _ := cmd.Flags().GetString("schema-path")
			workers, _ := cmd.Flags().GetInt("workers")
			lazy_interval, _ := cmd.Flags().GetInt("lazy-interval")
			generator, _ := cmd.Flags().GetString("generator") //format (json/csv)//data type (records, stream)

			status_codes_acceptance := map[string]float64{
				//code:%
				"200": 100,
				"204": 100,
				"205": 100,
			}

			workload := config.Workload{
				Name:      "ingest",
				Container: rootCommandeer.container,
				Target:    rootCommandeer.dirPath,
				Payload:   payload,
				Workers:   workers,
				Generator: generator,
				Schema:    schema_path,
				Lazy:      lazy_interval,
			}

			globals := config.Global{
				Port: port,
				StatusCodesAcceptance: status_codes_acceptance,
			}

			var wg sync.WaitGroup
			wg.Add(1)
			get := tui.LatencyCollector{}
			put := tui.LatencyCollector{}
			//stat := tui.StatusesCollector{}
			e := &httpblaster.Executor{
				Workload:       workload,
				Host:           host,
				TLS_mode:       false,
				Globals:        globals,
				Ch_put_latency: put.New(160, 1),
				Ch_get_latency: get.New(160, 1),
				//Ch_statuses:stat.New(160,1),
			}

			e.Start(&wg)
			wg.Wait()
			_, err := e.Report()
			return err
		},
	}
	cmd.Flags().String("payload-path", "payload.csv", "payload data file path")
	cmd.Flags().String("schema-path", "schema.json", "payload schema file path")
	cmd.Flags().IntP("workers", "w", 1, "number of worker threads")
	cmd.Flags().IntP("lazy-interval", "l", 0, "delay betweeen requests in miliseconds")
	cmd.Flags().String("generator", "csv2kv", "generator type (csv2kv/json2kv/line2stream)")
	return cmd
}
