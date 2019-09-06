package gocloudurls

import (
	"errors"
	"net/url"
	"path"
	"strings"
)

// NormalizePubSubURL normalize URL for PubSub.
func NormalizePubSubURL(srcUrl string) (string, error) {
	if isAWSPubSub(srcUrl) {
		return normalizeAWSPubSub(srcUrl)
	} else if strings.HasPrefix(srcUrl, "gcppubsub://") {
		return normalizeGCPPubSub(srcUrl)
	}
	return srcUrl, nil
}

func isAWSPubSub(path string) bool {
	return strings.HasPrefix(path, "awssns:///") ||
		strings.HasPrefix(path, "awssqs://") ||
		strings.HasPrefix(path, "arn:aws:sns") ||
		strings.HasPrefix(path, "https://sqs.")
}

func normalizeAWSPubSub(srcUrl string) (string, error) {
	if strings.HasPrefix(srcUrl, "arn:aws:sns") {
		fragments := strings.Split(srcUrl, ":")
		return "awssns:///" + srcUrl + "?region=" + fragments[3], nil
	} else if strings.HasPrefix(srcUrl, "awssns:///") {
		u, err := url.Parse(srcUrl)
		if err != nil {
			return "", err
		}
		if _, ok := u.Query()["region"]; !ok {
			fragments := strings.Split(srcUrl, ":")
			q := url.Values{}
			q.Set("region", fragments[4])
			u.RawQuery = q.Encode()
		}
		return u.String(), nil
	} else if strings.HasPrefix(srcUrl, "awssqs://https://") || strings.HasPrefix(srcUrl, "https://sqs.") {
		if strings.HasPrefix(srcUrl, "awssqs://https://") {
			srcUrl = srcUrl[len("awssqs://"):]
		}
		u, err := url.Parse(srcUrl)
		if err != nil {
			return "", err
		}
		if _, ok := u.Query()["region"]; !ok {
			fragments := strings.Split(u.Host, ".")
			q := url.Values{}
			q.Set("region", fragments[1])
			u.RawQuery = q.Encode()
		}
		return "awssqs://" + u.String(), nil
	}
	return srcUrl, nil
}

func normalizeGCPPubSub(p string) (string, error) {
	u, err := url.Parse(p)
	if err != nil {
		return "", err
	}
	fragments := strings.Split(u.Path, "/")
	switch u.Host {
	case "":
		return "", errors.New("gcppubsub url should have project and topic names")
	case "projects":
		if len(fragments) != 4 {
			return "", errors.New("gcppubsub url should have project and topic names")
		}
	default:
		if len(fragments) != 2 {
			return "", errors.New("gcppubsub url should have project and topic names")
		}
		u.Path = path.Join("/", u.Host, "topics", fragments[1])
		u.Host = "projects"
		p = u.String()
	}
	return p, nil
}
