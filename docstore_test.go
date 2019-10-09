package gocloudurls

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFixFirestore(t *testing.T) {
	testcases := []struct {
		name       string
		hasError   bool
		src        string
		collection string
		keyName    string
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
			name:       "with Collection",
			hasError:   false,
			src:        "firestore://projects/my-project/databases/my-database/documents/jobs",
			collection: "",
			expected:   "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=_id",
		},
		{
			name:       "with Collection: too short error",
			hasError:   true,
			src:        "firestore://projects/my-project/databases/my-database/documents",
			collection: "",
		},
		{
			name:       "with Collection: too long error",
			hasError:   true,
			src:        "firestore://projects/my-project/databases/my-database/documents/jobs/test",
			collection: "",
		},
		{
			name:       "with Collection (short form)",
			hasError:   false,
			src:        "firestore://my-project/my-database/jobs",
			collection: "",
			expected:   "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=_id",
		},
		{
			name:       "with Collection (short form): too short error",
			hasError:   true,
			src:        "firestore://my-project/my-database",
			collection: "",
		},
		{
			name:       "with Collection (short form): too long error",
			hasError:   true,
			src:        "firestore://my-project/my-database/jobs/test",
			collection: "",
		},
		{
			name:       "with name filed",
			hasError:   false,
			src:        "firestore://projects/my-project/databases/my-database/documents/jobs",
			collection: "",
			keyName:    "name",
			expected:   "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=name",
		},
		{
			name:       "with Collection and name filed",
			hasError:   false,
			src:        "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=id",
			collection: "",
			expected:   "firestore://projects/my-project/databases/my-database/documents/jobs?name_field=id",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			u, _ := url.Parse(testcase.src)
			result, err := normalizeFirestore(u, testcase.keyName, testcase.collection)
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
		name          string
		src           string
		keyName       string
		collection    string
		filename      string
		revisionField string
		hasError      bool
		expected      string
	}{
		{
			name:       "with inline Collection",
			src:        "mem://jobs",
			keyName:    "_id",
			hasError:   false,
			collection: "",
			expected:   "mem://jobs/_id",
		},
		{
			name:       "without inline Collection",
			src:        "mem://",
			keyName:    "_id",
			hasError:   false,
			collection: "jobs",
			expected:   "mem://jobs/_id",
		},
		{
			name:       "error if both URL and Collection parameter are empty",
			src:        "mem://",
			keyName:    "_id",
			hasError:   true,
			collection: "",
		},
		{
			name:       "keep query",
			src:        "mem://?filename=local.memdb&revision_field=updated_at",
			keyName:    "_id",
			hasError:   false,
			collection: "jobs",
			expected:   "mem://jobs/_id?filename=local.memdb&revision_field=updated_at",
		},
		{
			name:          "filename, revision_field",
			src:           "mem://",
			keyName:       "_id",
			hasError:      false,
			collection:    "jobs",
			filename:      "local.memdb",
			revisionField: "revision",
			expected:      "mem://jobs/_id?filename=local.memdb&revision_field=revision",
		},
		{
			name:       "key is already in path",
			src:        "mem://collections/key",
			keyName:    "",
			hasError:   false,
			collection: "",
			expected:   "mem://collections/key",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			u, _ := url.Parse(testcase.src)
			result, err := normalizeMemstore(u, testcase.keyName, testcase.collection, testcase.filename, testcase.revisionField)
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
		keyName      string
		partitionKey string
		hasError     bool
		expected     string
	}{
		{
			name:         "simple",
			src:          "dynamodb:",
			collection:   "tasks",
			keyName:      "",
			partitionKey: "",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=_id",
		},
		{
			name:         "keyName becomes parition key",
			src:          "dynamodb:",
			collection:   "tasks",
			keyName:      "key",
			partitionKey: "job_id",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=job_id&sort_key=key",
		},
		{
			name:         "sort key",
			src:          "dynamodb:",
			collection:   "tasks",
			keyName:      "",
			partitionKey: "job_id",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=job_id&sort_key=_id",
		},
		{
			name:         "with Collection",
			src:          "dynamodb://tasks",
			collection:   "",
			keyName:      "",
			partitionKey: "",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=_id",
		},
		{
			name:         "sort key with Collection",
			src:          "dynamodb://tasks",
			collection:   "",
			partitionKey: "job_id",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=job_id&sort_key=_id",
		},
		{
			name:         "partitionKey is in query",
			src:          "dynamodb://tasks?partition_key=name",
			collection:   "",
			keyName:      "",
			partitionKey: "",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=name",
		},
		{
			name:         "overwrite partitionKey is in query",
			src:          "dynamodb://tasks?partition_key=name",
			collection:   "",
			keyName:      "_id",
			partitionKey: "",
			hasError:     false,
			expected:     "dynamodb://tasks?partition_key=_id",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			u, _ := url.Parse(testcase.src)
			result, err := normalizeDynamo(u, testcase.keyName, testcase.partitionKey, testcase.collection)
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
		keyName    string
		hasError   bool
		expected   string
	}{
		{
			name:       "simple",
			src:        "mongo://my-db",
			collection: "tasks",
			keyName:    "",
			hasError:   false,
			expected:   "mongo://my-db/tasks?id_field=_id",
		},
		{
			name:       "specify id_field",
			src:        "mongo://my-db",
			collection: "tasks",
			keyName:    "name",
			hasError:   false,
			expected:   "mongo://my-db/tasks?id_field=name",
		},
		{
			name:       "with Collection",
			src:        "mongo://my-db/tasks",
			collection: "",
			keyName:    "",
			hasError:   false,
			expected:   "mongo://my-db/tasks?id_field=_id",
		},
		{
			name:       "with Collection and id_field",
			src:        "mongo://my-db/tasks?id_field=id",
			collection: "",
			keyName:    "",
			hasError:   false,
			expected:   "mongo://my-db/tasks?id_field=id",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			u, _ := url.Parse(testcase.src)
			result, err := normalizeMongo(u, testcase.keyName, testcase.collection)
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
			hasError:   false,
		},
		{
			name:       "MongoDB",
			src:        "mongo://my-db",
			collection: "tasks",
			expected:   "mongo://my-db/tasks?id_field=_id",
			hasError:   false,
		},
		{
			name:       "Firestore",
			src:        "firestore://my-project/my-database",
			collection: "tasks",
			expected:   "firestore://projects/my-project/databases/my-database/documents/tasks?name_field=_id",
			hasError:   false,
		},
		{
			name:       "Mem",
			src:        "mem://",
			collection: "tasks",
			expected:   "mem://tasks/_id",
			hasError:   false,
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			result, err := NormalizeDocStoreURL(testcase.src, Option{
				Collection: testcase.collection,
				KeyName:    "_id",
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
