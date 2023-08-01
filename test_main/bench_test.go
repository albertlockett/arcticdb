package main

import (
	"context"
	"os"
	"testing"

	logkit "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/polarsignals/frostdb"
)

func BenchmarkReplay2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		logger := logkit.NewLogfmtLogger(os.Stderr)
		logger = level.NewFilter(logger,
			level.AllowInfo(),
			// level.AllowWarn(),
			// level.AllowError(),
		)
		// log.Println("opening CS")

		columnstore, err := frostdb.New(
			// frostdb.WithLogger(logger),
			frostdb.WithStoragePath("/home/albertlockett/Development/arcticdb/tmp/"),
			frostdb.WithWAL(),
		)
		// log.Println("here")
		if err != nil {
			panic(err)
		}
		// require.NoError(b, err)
		// log.Println("opening DB")
		db, err := columnstore.DB(context.Background(), "simple_db")
		if err != nil {
			panic(err)
		}
		db.Close()
		columnstore.Close()
		// require.NoError(b, err)
		// log.Println("hello, world!")
	}
}
