package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime/pprof"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/polarsignals/frostdb"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

type FirstLast struct {
	FirstName string
	Surname   string
}

type Simple struct {
	Names FirstLast
	Value int64
}

func main() {
	// Start CPU profiling
	f, err := os.Create("cpu.pprof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	logger := log.NewLogfmtLogger(os.Stderr)
	logger = level.NewFilter(logger,
		level.AllowInfo(),
		// level.AllowWarn(),
		// level.AllowError(),
	)

	// Create a new column store
	columnstore, err := frostdb.New(
		frostdb.WithLogger(logger),
		frostdb.WithStoragePath("/home/albertlockett/Development/arcticdb/tmp/"),
		frostdb.WithWAL(),
	)
	if err != nil {
		panic(err)
	}
	defer columnstore.Close()

	// Open up a database in the column store
	_, err = columnstore.DB(context.Background(), "simple_db")
	if err != nil {
		panic(err)
	}

	// Define our simple schema of labels and values
	// schema := simpleSchema()

	// // Create a table named simple in our database
	// table, _ := database.Table(
	// 	"simple_table",
	// 	frostdb.NewTableConfig(schema),
	// )

	// for i := 0; i < 100000; i++ {
	// 	record := Simple{
	// 		Names: FirstLast{
	// 			FirstName: "Albert",
	// 			Surname:   "Lockett",
	// 		},
	// 		Value: 99,
	// 	}
	// 	// _, _ :=
	// 	table.Write(context.Background(), record)
	// }

	execBashScript()

}

func execBashScript() {
	thisPid := os.Getpid()
	cmd := exec.Command("/home/albertlockett/Development/arcticdb/test_main/cpu_time.sh", fmt.Sprintf("%d", thisPid))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

func simpleSchema() *schemapb.Schema {
	return &schemapb.Schema{
		Name: "simple_schema",
		Columns: []*schemapb.Column{{
			Name: "names",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				Nullable: true,
			},
			Dynamic: true,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "names",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	}
}

func generateNumbers(n int) []int {
	numbers := make([]int, n)
	for i := 0; i < n; i++ {
		numbers[i] = rand.Intn(100)
	}
	return numbers
}

func sumNumbers(numbers []int) int {
	sum := 0
	for _, n := range numbers {
		sum += n
	}
	return sum
}
