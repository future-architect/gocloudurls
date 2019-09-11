package gocloudurls

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
)

// Option is a option for NormalizeDocStoreURL
//
// KeyName is a primary key of document store. Default value is _id.
//
// PartitionKey is only for DynamoDB. If this parameter is specified, KeyName is used as a sort key.
// If PartitionKey is not specified, KeyName is used as a partitionKey.
//
// If Collection is specified, it returns URL for the collection. It is good for applications that uses multiple
// collections.
type Option struct {
	KeyName      string
	PartitionKey string
	Collection   string
}

// NormalizeDocStoreURL normalizes Document Store URL
//
// Usually, application uses multiple document collections (≒ table in RDB).
// So it provides API to replace collection name by application code (config specify until DB location).
//
// Default ``KeyName`` is ``"_id"`` as same as MongoDB.
//
// If ``PartitionKey`` is specified for DynamoDB, ``KeyName`` is specified as ``sort_key``.
// This config is ignored for other DocStores.
//
// Examples:
//
//   goclodurls.NormalizePubSubURL("mem://", goclodurls.Option{
//       Collection: "addresses",
//   })
//   // "mem://addresses/_id"
//
//   goclodurls.NormalizePubSubURL("firestore://my-project", goclodurls.Option{
//       Collection: "addresses",
//   })
//   // "firestore://projects/my-project/databases/(default)/documents/addresses?name_field=_id"
//
//   goclodurls.NormalizePubSubURL("firestore://my-project/my-documents/addresses", goclodurls.Option{})
//   // "firestore://projects/my-project/databases/my-documents/documents/addresses?name_field=_id"
//
//   goclodurls.NormalizePubSubURL("dynamodb://", goclodurls.Option{
//       Collection: "tasks",
//   })
//   // "dynamodb://tasks?partition_key=_id"
//
//   goclodurls.NormalizePubSubURL("dynamodb://", goclodurls.Option{
//       Collection:   "tasks",
//       PartitionKey: "job_id"
//   })
//   // "dynamodb://tasks?partition_key=job_id&sort_key=_id"
func NormalizeDocStoreURL(srcUrl string, opt Option) (string, error) {
	if opt.KeyName == "" {
		opt.KeyName = "_id"
	}
	u, err := url.Parse(srcUrl)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "mem":
		return normalizeMemstore(u, opt.KeyName, opt.Collection)
	case "firestore":
		return normalizeFirestore(u, opt.KeyName, opt.Collection)
	case "dynamodb":
		return normalizeDynamo(u, opt.KeyName, opt.PartitionKey, opt.Collection)
	case "mongo":
		return normalizeMongo(u, opt.KeyName, opt.Collection)
	}
	return "", fmt.Errorf("Unknown scheme of docstore: '%s'", u.Scheme)
}

// MustNormalizeDocStoreURL is similar to NormalizeDocStoreURL but raise panic if there is error
func MustNormalizeDocStoreURL(srcUrl string, opt Option) string {
	result, err := NormalizeDocStoreURL(srcUrl, opt)
	if err != nil {
		panic(err)
	}
	return result
}

func normalizeMemstore(u *url.URL, keyName, collection string) (string, error) {
	if collection == "" && u.Host == "" {
		return "", errors.New("opt.Collection is required if source URL doesn't have collection")
	}
	if collection != "" {
		u.Host = collection
	}
	u.Path = keyName
	return u.String(), nil
}

func normalizeFirestore(u *url.URL, keyName, collection string) (string, error) {
	if collection == "" {
		return normalizeFirestoreWithInnerCollection(u, keyName)
	} else {
		return normalizeFirestoreWithOuterCollection(u, keyName, collection)
	}
}

func normalizeFirestoreWithInnerCollection(u *url.URL, keyName string) (string, error) {
	u, _ = url.Parse(u.String())
	if u.Host == "" {
		return "", fmt.Errorf("Firestore URL doesn't have project information: %s", u.String())
	} else if u.Host != "projects" {
		u.Path = path.Join("/", u.Host, u.Path)
		u.Host = "projects"
	}
	elements := strings.Split(u.Path, "/")
	switch len(elements) {
	case 4:
		u.Path = path.Join("/", elements[1], "databases", elements[2], "documents", elements[3])
	case 6:
		u.Path = path.Join("/", elements[1], "databases", elements[3], "documents", elements[5])
	default:
		return "", fmt.Errorf("Firestroe URL should be firestore://(prj)/(db)/(docs) or firestore://projects/(prj)/databases/(db)/documents/(docs), but '%s'", u.String())
	}
	query := make(url.Values)
	query.Set("name_field", keyName)
	u.RawQuery = query.Encode()

	return strings.Replace(u.String(), "%28default%29", "(default)", 1), nil
}

func normalizeFirestoreWithOuterCollection(u *url.URL, keyName, collection string) (string, error) {
	u, _ = url.Parse(u.String())
	if u.Host == "" {
		return "", fmt.Errorf("Firestore URL doesn't have project information: %s", u.String())
	} else if u.Host != "projects" {
		u.Path = path.Join("/", u.Host, u.Path)
		u.Host = "projects"
	}
	elements := strings.Split(u.Path, "/")
	switch len(elements) {
	case 2:
		u.Path = path.Join("/", elements[1], "databases", "(default)", "documents", collection)
	case 3:
		u.Path = path.Join("/", elements[1], "databases", elements[2], "documents", collection)
	case 4:
		fallthrough
	case 5:
		u.Path = path.Join("/", elements[1], "databases", elements[3], "documents", collection)
	default:
		return "", fmt.Errorf("Firestroe URL should be firestore://(project) or firestore://(project)/(database) or firestore://projects/(project)/databases/(database)/documents, but '%s'", u.String())
	}
	query := make(url.Values)
	query.Set("name_field", keyName)
	u.RawQuery = query.Encode()

	return strings.Replace(u.String(), "%28default%29", "(default)", 1), nil
}

func normalizeDynamo(u *url.URL, keyName, partitionKey, collection string) (string, error) {
	if u.Host == "" && collection == "" {
		return "", errors.New("opt.Collection is required if source URL doesn't have collection")
	}
	u.Scheme = "dynamodb"
	if collection != "" {
		u.Host = collection
	}
	query := make(url.Values)
	if partitionKey != "" {
		query.Set("partition_key", partitionKey)
		query.Set("sort_key", "_id")
	} else {
		query.Set("partition_key", "_id")
	}
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func normalizeMongo(u *url.URL, keyName, collection string) (string, error) {
	if u.Host == "" {
		return "", errors.New("mongo requires hostname as a database name, but empty")
	}
	if u.Path == "/" && collection == "" {
		return "", errors.New("opt.Collection is required if source URL doesn't have collection")
	}
	u, _ = url.Parse(u.String())
	if u.Path == "" {
		u.Path = collection
	}
	query := make(url.Values)
	query.Set("id_field", "_id")
	u.RawQuery = query.Encode()
	return u.String(), nil
}
