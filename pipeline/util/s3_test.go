//go:build integration

package util

import (
	"testing"
	"time"

	"github.com/kaiachain/kaia/pipeline/types"
)

func TestS3(t *testing.T) {
	s3client, err := NewS3Client("ap-northeast-1")
	if err != nil {
		t.Errorf("Expected s3 client, got nil")
	}
	// res, err := s3client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	// if err != nil {
	// 	t.Errorf("Expected error, got nil")
	// }
	// if res == nil {
	// 	t.Errorf("Expected response, got nil")
	// }
	// t.Logf("Tested NewS3Client() successfully, %+v", res)
	b, err := DownloadFileFromS3(s3client, "chaintable-pipeline-test", "1/0xaeaa51f5cf80c31e3c21a80e5b2399e31675fe253c676d351db66aa35d341374")
	if err != nil {
		t.Errorf("Expected error, got nil")
	}
	decodeStartTime := time.Now()
	file := types.BlockFile{}
	err = DecodeFromGzipJson(b, &file)
	if err != nil {
		t.Errorf("Expected error, got nil")
	}
	t.Logf("DecodeFromGzipJson() took %v", time.Since(decodeStartTime))

	encodeStartTime := time.Now()
	// t.Logf("Tested DownloadFileFromS3() successfully, %+v", file)
	EncodeToJsonGzip(file)
	t.Logf("EncodeToJsonGzip() took %v", time.Since(encodeStartTime))
	// t.Logf("Tested DownloadFileFromS3() successfully, %+v", file)
}
