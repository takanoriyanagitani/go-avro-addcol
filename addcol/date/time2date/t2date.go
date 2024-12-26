package time2date

import (
	"context"
	"errors"
	"iter"
	"strings"
	"time"

	. "github.com/takanoriyanagitani/go-avro-addcol/util"
)

var (
	ErrInvalidTime error = errors.New("invalid time")
)

type TimeToDate func(time.Time) IO[string]

func TimeToDateFromLayout(layout string) TimeToDate {
	var bldr strings.Builder
	var buf [128]byte
	return func(t time.Time) IO[string] {
		return func(_ context.Context) (string, error) {
			bldr.Reset()
			var b []byte = buf[:0]
			var appended []byte = t.AppendFormat(b, layout)
			_, _ = bldr.Write(appended) // error is always nil or OOM
			return bldr.String(), nil
		}
	}
}

func TimeToDateDefault() TimeToDate {
	return TimeToDateFromLayout(time.DateOnly)
}

func (t TimeToDate) AnyToDate(a any) IO[string] {
	return func(ctx context.Context) (string, error) {
		switch c := a.(type) {
		case time.Time:
			return t(c)(ctx)
		default:
			return "", ErrInvalidTime
		}
	}
}

func (t TimeToDate) MapsToMaps(
	original iter.Seq2[map[string]any, error],
	timeColumnName string,
	dateColumnName string,
) IO[iter.Seq2[map[string]any, error]] {
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			buf := map[string]any{}
			for row, e := range original {
				clear(buf)

				if nil != e {
					yield(buf, e)
					return
				}

				for key, val := range row {
					buf[key] = val

					if timeColumnName == key {
						dateVal, e := t.AnyToDate(val)(ctx)
						if nil != e {
							yield(buf, e)
							return
						}

						buf[dateColumnName] = dateVal
					}
				}

				if !yield(buf, nil) {
					return
				}
			}
		}, nil
	}
}

type Config struct {
	TimeColumnName string
	DateColumnName string
}

func (c Config) CreateMapsToMaps(
	t2d TimeToDate,
) func(iter.Seq2[map[string]any, error]) IO[iter.Seq2[map[string]any, error]] {
	return func(
		original iter.Seq2[map[string]any, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return t2d.MapsToMaps(
			original,
			c.TimeColumnName,
			c.DateColumnName,
		)
	}
}
