package pubsub

import (
	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

type kinesisDescribeStream interface {
	DescribeStream(*kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error)
}

type kinesisPubSub interface {
	kinesisDescribeStream
	PutRecords(*kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

// gatherShards collects all shards for a given stream.
func gatherShards(c kinesisDescribeStream, streamName *string) ([]*kinesis.Shard, error) {
	var s []*kinesis.Shard
	r := kinesis.DescribeStreamInput{StreamName: streamName}
	more := true
	for more {
		d, err := c.DescribeStream(&r)
		if err != nil {
			return nil, err
		}
		s = append(s, d.StreamDescription.Shards...)
		r.ExclusiveStartShardID = s[len(s)-1].ShardID
		more = *d.StreamDescription.HasMoreShards
	}
	return s, nil
}

// explicitHashKeys collects explicit hash keys for all provided shards.
func explicitHashKeys(shards []*kinesis.Shard) []*string {
	return make([]*string, 0)
}

// fanOutPutRecordInput transforms a PutRecordInput to a PutRecordsInput which sends the same data to all explicit hash keys specified.
func fanOutPutRecordInput(c *kinesis.PutRecordInput, shards []*string) *kinesis.PutRecordsInput {
	return nil
}

// PutRecord takes in kinesis.PutRecordInput request and sends it to all shards in the Kinesis stream.
func PutRecord(c kinesisPubSub, input *kinesis.PutRecordInput) (*kinesis.PutRecordsOutput, error) {
	// XXX Does the caller worry about the transaction rate or this function?
	s, err := gatherShards(c, input.StreamName)
	if err != nil {
		return nil, err
	}
	k := explicitHashKeys(s)
	p := fanOutPutRecordInput(input, k)
	return c.PutRecords(p)
}
