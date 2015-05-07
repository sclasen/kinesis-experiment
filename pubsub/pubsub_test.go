package pubsub

import (
	"errors"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"testing"
)

type kinesisDescribeStreamMock struct {
	Index  int
	err    error
	Shards [][]*kinesis.Shard
}

func (c *kinesisDescribeStreamMock) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	if c.err != nil {
		return nil, c.err
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
		input := kinesisDescribeStreamMock{Shards: tt.value, err: tt.err}
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
