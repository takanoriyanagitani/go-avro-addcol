package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	. "github.com/takanoriyanagitani/go-avro-addcol/util"

	dt "github.com/takanoriyanagitani/go-avro-addcol/addcol/date/time2date"

	dh "github.com/takanoriyanagitani/go-avro-addcol/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-addcol/avro/enc/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var time2date dt.TimeToDate = dt.TimeToDateDefault()

var timeColName IO[string] = EnvValByKey("ENV_TIME_COLNAME")
var dateColName IO[string] = EnvValByKey("ENV_DATE_COLNAME")

var t2dcfg IO[dt.Config] = Bind(
	All(
		timeColName,
		dateColName,
	),
	Lift(func(s []string) (dt.Config, error) {
		return dt.Config{
			TimeColumnName: s[0],
			DateColumnName: s[1],
		}, nil
	}),
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	t2dcfg,
	func(cfg dt.Config) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			cfg.CreateMapsToMaps(time2date),
		)
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}
		defer f.Close()

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(schema string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(schema),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
