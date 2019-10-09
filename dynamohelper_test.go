package gocloudurls

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type TestStruct struct {
	Name string `docstore:"name"`
	Age  int
	Hash string `docstore:"-"`
}

func TestDocStoreSchema(t *testing.T) {
	ds, err := NewDynamoDBSchema(&TestStruct{}, "dynamodb://tasks?partition_key=name")
	assert.NotNil(t, ds)
	assert.Nil(t, err)
	if err == nil {
		assert.Equal(t, "tasks", ds.Collection)
		assert.NotNil(t, ds.PartitionKeyField)
		if ds.PartitionKeyField != nil {
			assert.Equal(t, "name", ds.PartitionKeyField.Name)
			assert.Equal(t, "S", ds.PartitionKeyField.Type)
		}
		assert.Nil(t, ds.SortKeyField)
		assert.Equal(t, []string{
			"aws", "dynamodb", "create-table", "--table-name", "tasks",
			"--attribute-definitions", "AttributeName=name,AttributeType=S",
			"--key-schema", "AttributeName=name,KeyType=HASH",
			"--provisioned-throughput", "ReadCapacityUnits=5,WriteCapacityUnits=5",
		}, ds.CreateTableCommand())
	} else {
		t.Log(err)
	}
}

func Test_detectDynamoType(t *testing.T) {
	type args struct {
		t reflect.Type
	}
	tests := []struct {
		name     string
		args     args
		want     string
		hasError bool
	}{
		{
			name: "time.Time instance",
			args: args{
				t: reflect.ValueOf(time.Now()).Type(),
			},
			want: "S",
		},
		{
			name: "time.Time pointer",
			args: args{
				t: reflect.ValueOf(&[]time.Time{time.Now()}[0]).Type(),
			},
			want: "S",
		},
		{
			name: "int",
			args: args{
				t: reflect.ValueOf(1).Type(),
			},
			want: "N",
		},
		{
			name: "string",
			args: args{
				t: reflect.ValueOf("hello").Type(),
			},
			want: "S",
		},
		{
			name: "bytes",
			args: args{
				t: reflect.ValueOf([]byte("hello")).Type(),
			},
			want: "B",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := detectDynamoType(tt.args.t)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				if got != tt.want {
					t.Errorf("isDate() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
