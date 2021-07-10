// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3sqs

import (
	"context"
	"io/ioutil"
	"sync"
	"testing"

	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestQueueReader(t *testing.T) {
	queue := make(chan *awssqs.Message, 1)
	queue <- newMessage()
	close(queue)

	// Create SQS reader mock
	sqs := new(MockReader)
	sqs.On("StartPolling", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((<-chan *awssqs.Message)(queue))
	sqs.On("Close").Return(nil)

	// Create S3 client mock
	orc, _ := ioutil.ReadFile("../../../test/test2.orc")
	var s3 MockLoader = func(context.Context, string) ([]byte, error) {
		return orc, nil
	}

	// Create new storage
	storage := NewWith(sqs, s3, monitor.NewNoop())
	assert.NotNil(t, storage)
	defer storage.Close()

	// Range until we're done
	var wg sync.WaitGroup
	wg.Add(1)
	storage.Range(func(v string) bool {
		data, err := s3.Load(context.Background(), v)
		if err != nil {
			return false
		}

		assert.Equal(t, orc, data)
		wg.Done()
		return true
	})

	wg.Wait()
}

func newMessage() *awssqs.Message {
	evt := `{  
		"Records":[  
		   {  
			  "eventVersion":"2.0",
			  "eventSource":"aws:s3",
			  "awsRegion":"us-east-1",
			  "eventName":"event-type",
			  "userIdentity":{  
				 "principalId":"Amazon-customer-ID-of-the-user-who-caused-the-event"
			  },
			  "requestParameters":{  
				 "sourceIPAddress":"ip-address-where-request-came-from"
			  },
			  "responseElements":{  
				 "x-amz-request-id":"Amazon S3 generated request ID",
				 "x-amz-id-2":"Amazon S3 host that processed the request"
			  },
			  "s3":{  
				 "s3SchemaVersion":"1.0",
				 "configurationId":"ID found in the bucket notification configuration",
				 "bucket":{  
					"name":"bucket-name",
					"ownerIdentity":{  
					   "principalId":"Amazon-customer-ID-of-the-bucket-owner"
					},
					"arn":"bucket-ARN"
				 },
				 "object":{  
					"key":"object-key",
					"eTag":"object eTag",
					"versionId":"object version if bucket is versioning-enabled, otherwise null"     
				 }
			  }
		   }
		]
	}`

	return &awssqs.Message{
		Body: &evt,
	}
}
