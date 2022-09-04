package mocks

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/jaegertracing/jaeger/model"
)

var _ driver.ValueConverter = ConverterMock{}

type ConverterMock struct{}

func (conv ConverterMock) ConvertValue(v interface{}) (driver.Value, error) {
	switch t := v.(type) {
	case model.TraceID:
		return driver.Value(t.String()), nil
	case time.Time:
		return driver.Value(t), nil
	case time.Duration:
		return driver.Value(t.Nanoseconds()), nil
	case model.SpanID:
		return driver.Value(t), nil
	case string:
		return driver.Value(t), nil
	case []uint8:
		return driver.Value(t), nil
	case int64:
		return driver.Value(t), nil
	case uint64:
		return driver.Value(t), nil
	case int:
		return driver.Value(t), nil
	case []string:
		return driver.Value(fmt.Sprint(t)), nil
	default:
		return nil, fmt.Errorf("unknown type %T", t)
	}
}
