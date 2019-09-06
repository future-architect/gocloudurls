package gocloudurls

import (
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestFixFirestore(t *testing.T) {
	testcases := []struct {
		name       string
		hasError   bool
		src        string
		collection string
		expected   string
	}{
		{
			name:       "simple",
			hasError:   false,
			src:        "firestore://projects/my-project",
			collection: "jobs",
			expected:   "firestore://projects/my-project/databases/(default)/documents/jobs?name_field=_id",
		},
		{
			name:       "error: no project",
			hasError:   true,
			src:        "firestore://",
			collection: "jobs",
		},
		{
			name:       "only project",
			hasError:   false,
			src:        "firestore://my-project",
			collection: "jobs",
			expected:   "firestore://projects/my-project/databases/(default)/documents/jobs?name_field=_id",
		},
		{
			name:       "project and documents",
			hasError:   false,
			src:        "firestore://my-project/my-database",
			collection: "jobs",
			expected:   "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=_id",
		},
		{
			name:       "complete path (1)",
			hasError:   false,
			src:        "firestore://projects/my-project/databases/my-database/documents",
			collection: "jobs",
			expected:   "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=_id",
		},
		{
			name:       "complete path (2)",
			hasError:   false,
			src:        "firestore://projects/my-project/databases/my-database",
			collection: "jobs",
			expected:   "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=_id",
		},
		{
			name:       "too long",
			hasError:   true,
			src:        "firestore://projects/my-project/databases/my-database/documents/my-document",
			collection: "jobs",
		},
		{
			name:       "with collection",
			hasError:   false,
			src:        "firestore://projects/my-project/databases/my-database/documents/jobs",
			collection: "",
			expected:   "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=_id",
		},
		{
			name:       "with collection: too short error",
			hasError:   true,
			src:        "firestore://projects/my-project/databases/my-database/documents",
			collection: "",
		},
		{
			name:       "with collection: too long error",
			hasError:   true,
			src:        "firestore://projects/my-project/databases/my-database/documents/jobs/test",
			collection: "",
		},
		{
			name:       "with collection (short form)",
			hasError:   false,
			src:        "firestore://my-project/my-database/jobs",
			collection: "",
			expected:   "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=_id",
		},
		{
			name:       "with collection (short form): too short error",
			hasError:   true,
			src:        "firestore://my-project/my-database",
			collection: "",
		},
		{
			name:       "with collection (short form): too long error",
			hasError:   true,
			src:        "firestore://my-project/my-database/jobs/test",
			collection: "",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			u, _ := url.Parse(testcase.src)
			result, err := normalizeFirestore(u, "_id", testcase.collection)
			if !testcase.hasError {
				assert.Nil(t, err)
				assert.Equal(t, testcase.expected, result)
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}

func TestNormalizeMemstore(t *testing.T) {
	testcases := []struct {
		name       string
		src        string
		keyName    string
		collection string
		hasError   bool
		expected   string
	}{
		{
			name:       "with inline collection",
			src:        "mem://jobs",
			keyName:    "_id",
			hasError:   false,
			collection: "",
			expected:   "mem://jobs/_id",
		},
		{
			name:       "without inline collection",
			src:        "mem://",
			keyName:    "_id",
			hasError:   false,
			collection: "jobs",
			expected:   "mem://jobs/_id",
		},
		{
			name:       "error if both URL and collection parameter are empty",
			src:        "mem://",
			keyName:    "_id",
			hasError:   true,
			collection: "",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			u, _ := url.Parse(testcase.src)
			result, err := normalizeMemstore(u, testcase.keyName, testcase.collection)
			if testcase.hasError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, testcase.expected, result)
			}
		})
	}
}

func TestNormalizeDynamo(t *testing.T) {
	testcases := []struct {
		name         string
		src          string
		collection   string
		partitionKey string
		hasError     bool
		expected     string
	}{
		{
			name:         "simple",
			src:          "dynamodb:",
			collection:   "tasks",
			partitionKey: "",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=_id",
		},
		{
			name:         "sort key",
			src:          "dynamodb:",
			collection:   "tasks",
			partitionKey: "job_id",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=job_id&sort_key=_id",
		},
		{
			name:         "with collection",
			src:          "dynamodb://tasks",
			collection:   "",
			partitionKey: "",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=_id",
		},
		{
			name:         "sort key with collection",
			src:          "dynamodb://tasks",
			collection:   "",
			partitionKey: "job_id",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=job_id&sort_key=_id",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			u, _ := url.Parse(testcase.src)
			result, err := normalizeDynamo(u, "_id", testcase.partitionKey, testcase.collection)
			if !testcase.hasError {
				assert.Nil(t, err)
				assert.Equal(t, testcase.expected, result)
			} else {
			}
		})
	}
}

func TestNormalizeMongo(t *testing.T) {
	testcases := []struct {
		name       string
		src        string
		collection string
		hasError   bool
		expected   string
	}{
		{
			name:       "simple",
			src:        "mongo://my-db",
			collection: "tasks",
			hasError:   false,
			expected:   "mongo://my-db/tasks?id_field=_id",
		},
		{
			name:       "with collection",
			src:        "mongo://my-db/tasks",
			collection: "",
			hasError:   false,
			expected:   "mongo://my-db/tasks?id_field=_id",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			u, _ := url.Parse(testcase.src)
			result, err := normalizeMongo(u, "_id", testcase.collection)
			if testcase.hasError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, testcase.expected, result)
			}
		})
	}
}

func TestNormalizeDocStoreURL(t *testing.T) {
	testcases := []struct {
		name       string
		src        string
		collection string
		expected   string
		hasError   bool
	}{
		{
			name:       "DynamoDB",
			src:        "dynamodb://my-db",
			collection: "tasks",
			expected:   "dynamodb://tasks?partition_key=_id",
			hasError:    false,
		},
		{
			name:       "MongoDB",
			src:        "mongo://my-db",
			collection: "tasks",
			expected:   "mongo://my-db/tasks?id_field=_id",
			hasError:    false,
		},
		{
			name:       "Firestore",
			src:        "firestore://my-project/my-database",
			collection: "tasks",
			expected:   "firestore://projects/my-project/databases/my-database/documents/tasks?name_field=_id",
			hasError:    false,
		},
		{
			name:       "Mem",
			src:        "mem://",
			collection: "tasks",
			expected:   "mem://tasks/_id",
			hasError:    false,
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			result, err := NormalizeDocStoreURL(testcase.src, Option{
				Collection: testcase.collection,
				KeyName: "_id",
			})
			if testcase.hasError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, testcase.expected, result)
			}

		})
	}
}
