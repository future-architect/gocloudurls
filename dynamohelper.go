package gocloudurls

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"
)

type Field struct {
	Name string
	Type string
}

// DynamoDBSchema creates AWS CLI command to create table.
//
// For example you makes the following struct to handle DynamoDB record:
//
//   type Person struct {
//	     Name string `docstore:"name"`
//	     Age  int
//   }
//
//   ds, err := NewDynamoDBSchema(&Person{}, MustMustNormalizeDocStoreURL("dynamodb://persons"))
//   ds.CreateTableCommand()
//   // returns slice of string.
//   // "aws", "dynamodb", "create-table", "--table-name", "persons",
//	 // "--attribute-definitions", "AttributeName=name,AttributeType=S",
//	 // "--key-schema", "AttributeName=name,KeyType=HASH",
//	 // "--provisioned-throughput", "ReadCapacityUnits=5,WriteCapacityUnits=5",
type DynamoDBSchema struct {
	Collection        string
	PartitionKeyField *Field
	SortKeyField      *Field
}

// CreateTableCommand returns command line to create table.
func (d DynamoDBSchema) CreateTableCommand(opt ...SchemaOption) []string {
	var o SchemaOption
	if len(opt) > 0 {
		o = opt[0]
	}
	if o.ReadCapacityUnits == 0 {
		o.ReadCapacityUnits = 5
	}
	if o.WriteCapacityUnits == 0 {
		o.WriteCapacityUnits = 5
	}
	result := []string{
		"aws",
		"dynamodb",
		"create-table",
		"--table-name",
		d.Collection,
	}
	if d.SortKeyField != nil {
		result = append(result,
			"--attribute-definitions",
			fmt.Sprintf("AttributeName=%s,AttributeType=%s", d.PartitionKeyField.Name, d.PartitionKeyField.Type),
			fmt.Sprintf("AttributeName=%s,AttributeType=%s", d.SortKeyField.Name, d.SortKeyField.Type),
			"--key-schema",
			fmt.Sprintf("AttributeName=%s,KeyType=HASH", d.PartitionKeyField.Name),
			fmt.Sprintf("AttributeName=%s,KeyType=RANGE", d.SortKeyField.Name))
	} else {
		result = append(result,
			"--attribute-definitions",
			fmt.Sprintf("AttributeName=%s,AttributeType=%s", d.PartitionKeyField.Name, d.PartitionKeyField.Type),
			"--key-schema",
			fmt.Sprintf("AttributeName=%s,KeyType=HASH", d.PartitionKeyField.Name))
	}
	result = append(result,
		"--provisioned-throughput",
		fmt.Sprintf("ReadCapacityUnits=%d,WriteCapacityUnits=%d", o.ReadCapacityUnits, o.WriteCapacityUnits))
	return result
}

type SchemaOption struct {
	ReadCapacityUnits  int
	WriteCapacityUnits int
}

// NewDynamoDBSchema creates DynamoDBSchema from urlString(it should be a return of NormalizeDocStoreURL),
// CollectionEntity struct.
func NewDynamoDBSchema(collectionEntity interface{}, urlString string) (*DynamoDBSchema, error) {
	sv := reflect.ValueOf(collectionEntity).Elem()
	if sv.Type().Kind() != reflect.Struct {
		typeStr := reflect.ValueOf(collectionEntity).Type().String()
		return nil, fmt.Errorf("collectionEntity should be struct interface or its pointer but: %s", typeStr)
	}
	sanitizedUrl, err := NormalizeDocStoreURL(urlString)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(sanitizedUrl)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "dynamodb" {
		return nil, fmt.Errorf("Now NewDynamoDBSchema only support dynamodb: scheme, but %s", u.Scheme)
	}
	partitionKey := u.Query().Get("partition_key")
	sortKey := u.Query().Get("sort_key")

	result := &DynamoDBSchema{
		Collection: u.Host,
	}

	for i := 0; i < sv.Type().NumField(); i++ {
		f := sv.Type().Field(i)
		var fieldName string
		if tag, ok := f.Tag.Lookup("docstore"); ok {
			if strings.HasPrefix(tag, "-") {
				continue
			}
			fieldName = strings.Split(tag, ",")[0]
		} else {
			fieldName = f.Name
		}
		if partitionKey == fieldName {
			t, err := detectDynamoType(f.Type)
			if err != nil {
				return nil, fmt.Errorf("This type %s is not supported for dynamo partition key", f.Type.String())
			}
			result.PartitionKeyField = &Field{
				Name: fieldName,
				Type: t,
			}
		} else if sortKey == fieldName {
			t, err := detectDynamoType(f.Type)
			if err != nil {
				return nil, fmt.Errorf("This type %s is not supported for dynamo sort key", f.Type.String())
			}
			result.SortKeyField = &Field{
				Name: fieldName,
				Type: t,
			}
		}
	}
	return result, nil
}

func detectDynamoType(t reflect.Type) (string, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.String() == "time.Time" {
		return "S", nil
	} else if t.String() == "[]uint8" {
		return "B", nil
	}
	if dt, ok := typeMap[t.Kind()]; ok {
		return dt, nil
	}
	return "-", nil
}

var typeMap = map[reflect.Kind]string{
	reflect.String:  "S",
	reflect.Bool:    "BOOL",
	reflect.Int:     "N",
	reflect.Int8:    "N",
	reflect.Int16:   "N",
	reflect.Int32:   "N",
	reflect.Int64:   "N",
	reflect.Uint:    "N",
	reflect.Uint8:   "N",
	reflect.Uint16:  "N",
	reflect.Uint32:  "N",
	reflect.Uint64:  "N",
	reflect.Float32: "N",
	reflect.Float64: "N",
}
