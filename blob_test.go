package gocloudurls

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeBlobURL(t *testing.T) {
	testcases := []struct {
		name     string
		src      string
		hasError bool
		environs []string
		expected string
	}{
		{
			name:     "mem",
			src:      "mem://",
			hasError: false,
			expected: "mem:",
		},
		{
			name:     "mem 2",
			src:      "mem",
			hasError: false,
			expected: "mem:",
		},
		{
			name:     "file",
			src:      ".",
			hasError: false,
			expected: "file://.",
		},
		{
			name:     "gs",
			src:      "gs://my-bucket",
			hasError: false,
			expected: "gs://my-bucket",
		},
		{
			name:     "s3",
			src:      "s3://my-bucket?region=us-west-1",
			hasError: false,
			expected: "s3://my-bucket?region=us-west-1",
		},
		{
			name:     "s3 2",
			src:      "s3://my-bucket",
			hasError: false,
			expected: "s3://my-bucket?region=us-west-1",
			environs: []string{"AWS_REGION=us-west-1"},
		},
		{
			name:     "s3 error",
			src:      "s3://my-bucket",
			hasError: true,
			environs: []string{},
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			result, err := normalizeBlobURL(testcase.src, testcase.environs)
			if testcase.hasError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, testcase.expected, result)
			}
		})
	}
}
