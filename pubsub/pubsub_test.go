package pubsub

import (
	"errors"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"testing"
)

type kinesisDescribeStreamMock struct {
	Index  int
	err    error
	Shards []*kinesis.Shard
}

func (c *kinesisDescribeStreamMock) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	if c.err != nil {
		return nil, c.err
	}
	s := []*kinesis.Shard{c.Shards[c.Index]}
	c.Index++
	m := c.Index < len(c.Shards)
	d := kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			HasMoreShards: &m,
			Shards:        s,
			StreamName:    input.StreamName,
		}}
	return &d, nil
}

func TestGatherShardsSingleCall(t *testing.T) {
	n := "test stream"
	id := "shard ID"
	want := &kinesis.Shard{ShardID: &id}
	s := []*kinesis.Shard{want}
	i := kinesisDescribeStreamMock{Shards: s}
	got, err := gatherShards(&i, &n)
	if err != nil {
		t.Error(err)
	} else if len(got) != 1 || *got[0] != *want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGatherShardsMultipleCalls(t *testing.T) {
	n := "test stream"
	id1 := "shard ID 1"
	id2 := "shard ID 2"
	s1 := &kinesis.Shard{ShardID: &id1}
	s2 := &kinesis.Shard{ShardID: &id2}
	want := []*kinesis.Shard{s1, s2}
	i := kinesisDescribeStreamMock{Shards: want}
	got, err := gatherShards(&i, &n)
	if err != nil {
		t.Error(err)
	} else if len(got) != 2 || *got[0] != *want[0] || *got[1] != *want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGatherShardsFailure(t *testing.T) {
	n := "test stream"
	i := kinesisDescribeStreamMock{err: errors.New("simulated failure")}
	if _, err := gatherShards(&i, &n); err == nil {
		t.Error("got %v, want %v", nil, err)
	}
}

func TestExplicitHashKeys(t *testing.T) {
	k1 := "key 1"
	k2 := "key 2"
	k3 := "key 3"
	want := []*string{&k1, &k2, &k3}
	var i []*kinesis.Shard
	for _, k := range want {
		i = append(i, &kinesis.Shard{HashKeyRange: &kinesis.HashKeyRange{StartingHashKey: k, EndingHashKey: k}})
	}
	got := explicitHashKeys(i)
	if len(got) != len(want) {
		t.Errorf("got %v, want %v", want, got)
	}
	for index := range want {
		if *got[index] != *want[index] {
			t.Errorf("got[%i] == %v, want %v", index, *got[index], *want[index])
		}
	}
}
