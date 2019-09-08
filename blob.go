package gocloudurls

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// NormalizeBlobURL normalize blob URL. environ assumes os.Environ().
//
// When region is not specified for S3, this function gets region information from AWS_REGION
// environment variable:
//
// If "mem" is specified, it returns "memblob" URL.
// It other names specified, it returns fileblob URL.
func NormalizeBlobURL(srcUrl string, environ []string) (string, error) {
	return normalizeBlobURL(srcUrl, os.Environ())
}

func normalizeBlobURL(srcUrl string, environ []string) (string, error) {
	u, err := url.Parse(srcUrl)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "":
		if u.Path == "mem" {
			u.Scheme = "mem"
			u.Path = ""
		} else {
			u.Scheme = "file"
			u.Host = u.Path
			u.Path = ""
		}
	case "s3":
		if _, ok := u.Query()["region"]; !ok {
			found := false
			for _, env := range environ {
				if strings.HasPrefix(env, "AWS_REGION=") {
					query := make(url.Values)
					query.Set("region", env[len("AWS_REGION="):])
					u.RawQuery = query.Encode()
					found = true
					break
				}
			}
			if !found {
				return "", fmt.Errorf("S3 URL '%s' doesn't have region query and no AWS_REGION env var", u.String())
			}
		}
	}
	return u.String(), nil
}
