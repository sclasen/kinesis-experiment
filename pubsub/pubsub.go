package pubsub

import (
	"errors"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

// PutRecord takes in kinesis.PutRecordInput request and sends it to all shards in the Kinesis stream.
func PutRecord(c *kinesis.Kinesis, input *kinesis.PutRecordInput) (output *kinesis.PutRecordsOutput, err error) {
	return nil, errors.New("not implemented")
}
