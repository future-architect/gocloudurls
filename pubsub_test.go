package gocloudurls

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNormalizeAWSPubSub(t *testing.T) {
	testcases := []struct {
		name     string
		src      string
		hasError bool
		environs []string
		expected string
	}{
		{
			name:     "SNS - FQDN",
			src:      "awssns:///arn:aws:sns:us-east-2:123456789012:mytopic?region=us-east-2",
			hasError: false,
			expected: "awssns:///arn:aws:sns:us-east-2:123456789012:mytopic?region=us-east-2",
		},
		{
			name:     "SNS - FQDN without region",
			src:      "awssns:///arn:aws:sns:us-east-2:123456789012:mytopic",
			hasError: false,
			expected: "awssns:///arn:aws:sns:us-east-2:123456789012:mytopic?region=us-east-2",
		},
		{
			name:     "SNS - ARN",
			src:      "arn:aws:sns:us-east-2:123456789012:mytopic",
			hasError: false,
			expected: "awssns:///arn:aws:sns:us-east-2:123456789012:mytopic?region=us-east-2",
		},
		{
			name:     "SQS - with awssqs shecme",
			src:      "awssqs://https://sqs.us-east-2.amazonaws.com/123456789012/myqueue?region=us-east-2",
			hasError: false,
			expected: "awssqs://https://sqs.us-east-2.amazonaws.com/123456789012/myqueue?region=us-east-2",
		},
		{
			name:     "SQS - without awssqs shecme",
			src:      "https://sqs.us-east-2.amazonaws.com/123456789012/myqueue",
			hasError: false,
			expected: "awssqs://https://sqs.us-east-2.amazonaws.com/123456789012/myqueue?region=us-east-2",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			result, err := normalizeAWSPubSub(testcase.src)
			if testcase.hasError {
				assert.NotNil(t, err)
			} else {
				assert.True(t, isAWSPubSub(testcase.src))
				assert.Nil(t, err)
				assert.Equal(t, testcase.expected, result)
			}
		})
	}
}

func TestFixCloudPubSub(t *testing.T) {
	testcases := []struct {
		name     string
		hasError bool
		src      string
		expected string
	}{
		{
			name:     "as is",
			hasError: false,
			src:      "gcppubsub://projects/myproject/topics/mytopic",
			expected: "gcppubsub://projects/myproject/topics/mytopic",
		},
		{
			name:     "short",
			hasError: false,
			src:      "gcppubsub://myproject/mytopic",
			expected: "gcppubsub://projects/myproject/topics/mytopic",
		},
		{
			name:     "error: no project",
			hasError: true,
			src:      "gcppubsub://",
		},
		{
			name:     "only project",
			hasError: true,
			src:      "gcppubsub://myproject",
		},
		{
			name:     "project and topic",
			hasError: false,
			src:      "gcppubsub://myproject/mytopic",
			expected: "gcppubsub://projects/myproject/topics/mytopic",
		},
		{
			name:     "too long",
			hasError: true,
			expected: "gcppubsub://projects/myproject/topics/mytopic/test",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			result, err := normalizeGCPPubSub(testcase.src)
			if !testcase.hasError {
				assert.Nil(t, err)
				assert.Equal(t, testcase.expected, result)
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}
