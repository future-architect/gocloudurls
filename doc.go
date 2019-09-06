// package gocloudurls helps gocloud.dev packages.
//
// gocloud.dev uses URL to initialize cloud resources like blob, docstore, pubsub and so on.
// This package normalize URLs of that:
//
//    snsSrcPath := "arn:aws:sns:us-east-2:123456789012:mytopic"
//    snsPath, err := gocloudurls.NormalizePubSubURL(snsSrcPath)
//    // -> "awssns:///arn:aws:sns:us-east-2:123456789012:mytopic?region=us-east-2"
//    topic, err := pubsub.OpenTopic(ctx, snsPath)
package gocloudurls
