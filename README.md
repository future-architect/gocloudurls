# gocloudurls

[![GoDoc](https://godoc.org/github.com/future-architect/gocloudurls?status.svg)](https://godoc.org/github.com/future-architect/gocloudurls)

gocloudurls package is a helper for gocloud.dev.

Now it provides three functions for

* PubSub
* DocStore
* Blob

## Purpose

It makes configuration easy for gocloud.dev.
gocloud.dev requires special form of URLs to specify cloud resources.
This package normalize more human readable/writable config values into gocloud.dev ones.


## Functions

### ``func NormalizeBlobURL(srcUrl string) (string, error)``

It normalize shorter version of blob URLs into gocloud.dev acceptable URLs.

Examples:

* ``mem`` → ``mem://``
* ``folder`` → ``file://folder``
* ``s3://my-bucket`` → ``s3://my-bucket?region=us-west-1``

It gets AWS region name form ``AWS_REGION`` environment variable that is acceptable in AWS Lambda.

``MustNormalizeBlobURL`` raise panic if there is error.

### ``func NormalizePubSubURL(srcUrl string) (string, error)``

It normalizes shorter version of PubSub/SQS/SNS identifier into gocloud.dev acceptable URLs.

Examples:

```go
gocloudurls.NormalizePubSubURL("arn:aws:sns:us-east-2:123456789012:mytopic")
// "awssns:///arn:aws:sns:us-east-2:123456789012:mytopic?region=us-east-2"
```

```go
gocloudurls.NormalizePubSubURL("https://sqs.us-east-2.amazonaws.com/123456789012/myqueue")
// "awssqs://https://sqs.us-east-2.amazonaws.com/123456789012/myqueue?region=us-east-2"
```

```go
gocloudurls.NormalizePubSubURL("gcppubsub://myproject/mytopic")
// "gcppubsub://projects/myproject/topics/mytopic"
```

``MustNormalizePubSubURL`` raise panic if there is error.

### ``func NormalizeDocStoreURL(srcUrl string, opt Option) (string, error)``

```go
type Option struct {
	KeyName      string
	PartitionKey string
	Collection   string
}
```

Usually, application uses multiple document collections (≒ table in RDB).
So it provides API to replace collection name by application code (config specify until DB location).

Default ``KeyName`` is ``"_id"`` as same as MongoDB.

If ``PartitionKey`` is specified for DynamoDB, ``KeyName`` is specified as ``sort_key``.
This config is ignored for other DocStores.

Examples:

```go
goclodurls.NormalizePubSubURL("mem://", goclodurls.Option{
    Collection: "addresses",
})
// "mem://addresses/_id"
```

```go
goclodurls.NormalizePubSubURL("firestore://my-project", goclodurls.Option{
    Collection: "addresses",
})
// "firestore://projects/my-project/databases/(default)/documents/addresses?name_field=_id"
```

```go
goclodurls.NormalizePubSubURL("firestore://my-project/my-documents/addresses", goclodurls.Option{})
// "firestore://projects/my-project/databases/my-documents/documents/addresses?name_field=_id"
```

```go
goclodurls.NormalizePubSubURL("dynamodb://", goclodurls.Option{
    Collection: "tasks",
})
// "dynamodb://tasks?partition_key=_id"
```

```go
goclodurls.NormalizePubSubURL("dynamodb://", goclodurls.Option{
    Collection:   "tasks",
    PartitionKey: "job_id"
})
// "dynamodb://tasks?partition_key=job_id&sort_key=_id"
```

``MustNormalizeDocStoreURL`` raise panic if there is error.

## Struct

``DynamoDBSchema`` creates AWS CLI command to create table.

For example you makes the following struct to handle DynamoDB record:

```go
type Person struct {
   Name string `docstore:"name"`
   Age  int
}
```

You can get AWS command options:

```go
ds, err := NewDynamoDBSchema(&Person{}, MustMustNormalizeDocStoreURL("dynamodb://persons"))
ds.CreateTableCommand()
// It returns slice of string.
// "aws", "dynamodb", "create-table", "--table-name", "persons",
// "--attribute-definitions", "AttributeName=name,AttributeType=S",
// "--key-schema", "AttributeName=name,KeyType=HASH",
// "--provisioned-throughput", "ReadCapacityUnits=5,WriteCapacityUnits=5",
```

If you can modify Read/Write capacity unis, you can use SchemaOption:

```go
ds.CreateTableCommand(SchemaOption{
    ReadCapacityUnits: 10,
    WriteCapacityUnits: 10,
})
```

## License

Apache 2
