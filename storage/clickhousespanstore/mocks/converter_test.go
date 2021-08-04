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

	testCases := map[string]struct {
		valueToConvert interface{}
		expectedResult driver.Value
	}{
		"string value":       {valueToConvert: "some string value", expectedResult: driver.Value("some string value")},
		"string slice value": {valueToConvert: []string{"some", "slice", "of", "strings"}, expectedResult: driver.Value("[some slice of strings]")},
		"time value": {
			valueToConvert: time.Date(2002, time.February, 19, 14, 43, 51, 0, time.UTC),
			expectedResult: driver.Value(time.Date(2002, time.February, 19, 14, 43, 51, 0, time.UTC)),
		},
		"duration value": {
			valueToConvert: time.Unix(12340, 123456789).Sub(time.Unix(0, 0)),
			expectedResult: driver.Value(int64(12340123456789)),
		},
		"int64 value":         {valueToConvert: int64(1823), expectedResult: driver.Value(int64(1823))},
		"model.SpanID value":  {valueToConvert: model.SpanID(318148), expectedResult: driver.Value(model.SpanID(318148))},
		"model.TraceID value": {valueToConvert: model.TraceID{Low: 0xabd5, High: 0xa31}, expectedResult: driver.Value("0000000000000a31000000000000abd5")},
		"uint8 slice value":   {valueToConvert: []uint8("asdkja"), expectedResult: driver.Value([]uint8{0x61, 0x73, 0x64, 0x6b, 0x6a, 0x61})},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			converted, err := converter.ConvertValue(test.valueToConvert)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedResult, converted)
		})
	}
}

func TestConverterMock_Fail(t *testing.T) {
	converter := ConverterMock{}

	tests := map[string]struct{
		valueToConvert interface{}
		expectedErrorMsg string
	} {
		"float64 value": {valueToConvert: float64(1e-4), expectedErrorMsg: "unknown type float64"},
		"int32 value": {valueToConvert: int32(12831), expectedErrorMsg: "unknown type int32"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			val, err := converter.ConvertValue(test.valueToConvert)
			assert.Equal(t, nil, val)
			assert.EqualError(t, err, test.expectedErrorMsg)
		})
	}
}
