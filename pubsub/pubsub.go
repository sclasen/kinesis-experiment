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
	var k []*string
	for _, s := range shards {
		// Shard.HashKeyRange.StaringHashKey and all intermediate values are required, so no need to check for existence.
		k = append(k, s.HashKeyRange.StartingHashKey)
	}
	return k
}

// fanOutPutRecordInput transforms a PutRecordInput to a PutRecordsInput which sends the same data to all explicit hash keys specified.
func fanOutPutRecordInput(input *kinesis.PutRecordInput, keys []*string) *kinesis.PutRecordsInput {
	var requests []*kinesis.PutRecordsRequestEntry
	for _, key := range keys {
		r := &kinesis.PutRecordsRequestEntry{Data: input.Data, ExplicitHashKey: key, /* PartitionKey */}
		requests = append(requests, r)
	}
	return &kinesis.PutRecordsInput{Records: requests, StreamName: input.StreamName}
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
