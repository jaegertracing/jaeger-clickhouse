package mocks

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
)

func TestConverterMock_ConvertValue(t *testing.T) {
	converter := ConverterMock{}

	type test struct {
		valueToConvert interface{}
		expectedResult driver.Value
	}

	testCases := map[string]test{
		"string value":       {"some string value", driver.Value("some string value")},
		"string slice value": {[]string{"some", "slice", "of", "strings"}, driver.Value("[some slice of strings]")},
		"time value": {
			time.Date(2002, time.February, 19, 14, 43, 51, 0, time.UTC),
			driver.Value(time.Date(2002, time.February, 19, 14, 43, 51, 0, time.UTC)),
		},
		"duration value": {
			time.Unix(12340, 123456789).Sub(time.Unix(0, 0)),
			driver.Value(int64(12340123456789)),
		},
		"int64 value":         {int64(1823), driver.Value(int64(1823))},
		"model.SpanID value":  {model.SpanID(318148), driver.Value(model.SpanID(318148))},
		"model.TraceID value": {model.TraceID{Low: 0xabd5, High: 0xa31}, driver.Value("0000000000000a31000000000000abd5")},
		"uint8 slice value":   {[]uint8("asdkja"), driver.Value([]uint8{0x61, 0x73, 0x64, 0x6b, 0x6a, 0x61})},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			converted, err := converter.ConvertValue(test.valueToConvert)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedResult, converted)
		})
	}
}
