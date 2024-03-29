package pubsub

import (
	"bytes"
	"errors"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"testing"
)

type kinesisDescribeStreamMock struct {
	Index  int
	Err    error
	Shards [][]*kinesis.Shard
}

func (c *kinesisDescribeStreamMock) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	s := c.Shards[c.Index]
	c.Index++
	m := c.Index < len(c.Shards)
	d := kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			HasMoreShards: &m,
			Shards:        s,
			StreamName:    input.StreamName,
		},
	}
	return &d, nil
}

func TestGatherShards(t *testing.T) {
	id1 := "shard ID 1"
	id2 := "shard ID 2"
	id3 := "shard ID 3"
	id4 := "shard ID 4"
	s1 := kinesis.Shard{ShardID: &id1}
	s2 := kinesis.Shard{ShardID: &id2}
	s3 := kinesis.Shard{ShardID: &id3}
	s4 := kinesis.Shard{ShardID: &id4}
	tests := []struct {
		value    [][]*kinesis.Shard
		expected []*kinesis.Shard
		err      error
	}{
		{
			// [[s1]]
			[][]*kinesis.Shard{[]*kinesis.Shard{&s1}},
			[]*kinesis.Shard{&s1},
			nil,
		},
		{
			// [[s1, s2]]
			[][]*kinesis.Shard{[]*kinesis.Shard{&s1, &s2}},
			[]*kinesis.Shard{&s1, &s2},
			nil,
		},
		{
			// [[s1], [s2, s3], [s4]]
			[][]*kinesis.Shard{[]*kinesis.Shard{&s1}, []*kinesis.Shard{&s2, &s3}, []*kinesis.Shard{&s4}},
			[]*kinesis.Shard{&s1, &s2, &s3, &s4},
			nil,
		},
		{
			// Error.
			nil,
			nil,
			errors.New("simulated error"),
		},
	}

	for _, tt := range tests {
		name := "test stream"
		input := kinesisDescribeStreamMock{Shards: tt.value, Err: tt.err}
		result, err := gatherShards(&input, &name)
		if len(result) != len(tt.expected) {
			t.Errorf("expected %v, was %v", tt.expected, result)
		}
		if err != tt.err {
			t.Errorf("error condition: expected %v, was %v", tt.err, err)
		}
		for i, expected := range tt.expected {
			shardResult := result[i]
			if *shardResult != *expected {
				t.Errorf("expected[%i] %v, was %v", i, *expected, *shardResult)
			}
		}
	}
}

func TestExplicitHashKeys(t *testing.T) {
	k1 := "key 1"
	k2 := "key 2"
	k3 := "key 3"
	want := []*string{&k1, &k2, &k3}
	var input []*kinesis.Shard
	for _, k := range want {
		input = append(input, &kinesis.Shard{HashKeyRange: &kinesis.HashKeyRange{StartingHashKey: k, EndingHashKey: k}})
	}
	got := explicitHashKeys(input)
	if len(got) != len(want) {
		t.Errorf("got %v, want %v", want, got)
	}
	for index := range want {
		if *got[index] != *want[index] {
			t.Errorf("got[%i] == %v, want %v", index, *got[index], *want[index])
		}
	}
}

func TestFanOutPutRecordInput(t *testing.T) {
	s := "stream name"
	d := []byte("blob payload")
	k1 := "key 1"
	k2 := "key 2"
	k3 := "key 3"
	keys := []*string{&k1, &k2, &k3}
	input := kinesis.PutRecordInput{Data: d, StreamName: &s}
	result, err := fanOutPutRecordInput(&input, keys)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if *result.StreamName != s {
		t.Errorf("expected stream name %s, was %s", s, *result.StreamName)
	}
	for i, e := range result.Records {
		if bytes.Compare(e.Data, d) != 0 {
			t.Errorf("expected data %v, was %s", d, e.Data)
		}
		expectedKey := keys[i]
		if *e.ExplicitHashKey != *expectedKey {
			t.Errorf("expected explicit hash key %s, was %s", *expectedKey, *e.ExplicitHashKey)
		}
	}
}

func TestFanOutPutRecordInputFailure(t *testing.T) {
	s := "stream name"
	num := "1234"
	d := []byte("blob payload")
	k1 := "key 1"
	keys := []*string{&k1}
	input := kinesis.PutRecordInput{Data: d, StreamName: &s, SequenceNumberForOrdering: &num}
	_, err := fanOutPutRecordInput(&input, keys)
	if err == nil {
		t.Error("expected an error, was nil")
	}
}

type kinesisPutRecordsMock struct {
	kinesisDescribeStreamMock
	PutRecordsInput *kinesis.PutRecordsInput
}

func (c *kinesisPutRecordsMock) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	c.PutRecordsInput = input
	return nil, nil
}

func TestPutRecord(t *testing.T) {
	var c kinesisPutRecordsMock
	id1 := "shard ID 1"
	id2 := "shard ID 2"
	id3 := "shard ID 3"
	id4 := "shard ID 4"
	k1 := "shard key 1"
	k2 := "shard key 2"
	k3 := "shard key 3"
	k4 := "shard key 4"
	expectedKeys := []*string{&k1, &k2, &k3, &k4}
	s1 := kinesis.Shard{ShardID: &id1, HashKeyRange: &kinesis.HashKeyRange{StartingHashKey: &k1, EndingHashKey: &k1}}
	s2 := kinesis.Shard{ShardID: &id2, HashKeyRange: &kinesis.HashKeyRange{StartingHashKey: &k2, EndingHashKey: &k2}}
	s3 := kinesis.Shard{ShardID: &id3, HashKeyRange: &kinesis.HashKeyRange{StartingHashKey: &k3, EndingHashKey: &k3}}
	s4 := kinesis.Shard{ShardID: &id4, HashKeyRange: &kinesis.HashKeyRange{StartingHashKey: &k4, EndingHashKey: &k4}}
	c.Shards = [][]*kinesis.Shard{[]*kinesis.Shard{&s1}, []*kinesis.Shard{&s2, &s3}, []*kinesis.Shard{&s4}}
	stream := "stream name"
	d := []byte("blob payload")
	input := kinesis.PutRecordInput{Data: d, StreamName: &stream}
	_, err := PutRecord(&c, &input)
	if err != nil {
		t.Errorf("unexpected error %s", err)
	}
	result := *c.PutRecordsInput
	if *result.StreamName != stream {
		t.Errorf("expected stream name %s, was %s", stream, *result.StreamName)
	}
	for i, e := range result.Records {
		if bytes.Compare(e.Data, d) != 0 {
			t.Errorf("expected data %v, was %s", d, e.Data)
		}
		expectedKey := expectedKeys[i]
		if *e.ExplicitHashKey != *expectedKey {
			t.Errorf("expected explicit hash key %s, was %s", *expectedKey, *e.ExplicitHashKey)
		}
	}
}

func TestPutRecordFailsGatherShards(t *testing.T) {
	var c kinesisPutRecordsMock
	error := errors.New("simulated gatherThreads error")
	c.Err = error
	s := "stream name"
	if _, err := PutRecord(&c, &kinesis.PutRecordInput{StreamName: &s}); err != error {
		t.Errorf("expected error %s, was %s", error, err)
	}
}

func TestPutRecordFailsFanOutPutRecordInput(t *testing.T) {
	var c kinesisPutRecordsMock
	id1 := "shard ID 1"
	k1 := "shard key 1"
	s1 := kinesis.Shard{ShardID: &id1, HashKeyRange: &kinesis.HashKeyRange{StartingHashKey: &k1, EndingHashKey: &k1}}
	c.Shards = [][]*kinesis.Shard{[]*kinesis.Shard{&s1}}
	stream := "stream name"
	num := "1234"
	d := []byte("blob payload")
	input := kinesis.PutRecordInput{Data: d, StreamName: &stream, SequenceNumberForOrdering: &num}
	if _, err := PutRecord(&c, &input); err == nil {
		t.Errorf("expected an error, was %s", err)
	}
}
