package api

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

// Poll(timeout)
// Commit() => once processed, commit the offset
// Seek() => seek to an offset in a partition
// Unsubscribe() => unsub from a topic
// Pause() => stop consumption

func newInMemoryBroker() (*PartitionWriter, Broker) {
	// given a broker with an in memory write strategy
	pw := NewPartitionWriter()
	b := NewKrakeBroker(pw)
	return pw, b
}

func TestKrakeBroker_Produce_MemoryLayout(t *testing.T) {
	pw, b := newInMemoryBroker()

	PartitionCount := 3

	// ... and a topic
	err := b.CreateTopic(TopicConfiguration{
		Name:            "my-topic",
		PartitionCount:  PartitionCount,
		RetentionPeriod: 24 * time.Hour,
	})
	assert.NoError(t, err)

	// when i produce {PartitionCount} messages
	for i := 0; i < PartitionCount; i++ {
		err = b.Produce("my-topic", &Message{
			Key:     nil,
			Message: []byte(fmt.Sprintf("message: %d", i)),
		})
		assert.NoError(t, err)
	}

	// then i have a message on each partition
	for i := 0; i < PartitionCount; i++ {
		seg := pw.ActiveSegment(int32(i))
		data, _ := io.ReadAll(seg)

		msg := fmt.Sprintf("message: %d", i)
		assert.Equal(t, msg, string(data))
	}
}

func TestKrakeBroker_CreateDuplicateTopics(t *testing.T) {
	_, b := newInMemoryBroker()

	err := b.CreateTopic(TopicConfiguration{
		Name:            "my-topic",
		PartitionCount:  3,
		RetentionPeriod: 2 * time.Hour,
	})

	assert.NoError(t, err)

	err = b.CreateTopic(TopicConfiguration{
		Name:            "my-topic",
		PartitionCount:  3,
		RetentionPeriod: 2 * time.Hour,
	})

	assert.Equal(t, ErrTopicAlreadyExists, err)
}

func TestKrakeBroker_CreateTopic(t *testing.T) {
	_, b := newInMemoryBroker()

	err := b.CreateTopic(TopicConfiguration{
		Name:            "my-topic",
		PartitionCount:  3,
		RetentionPeriod: 2 * time.Hour,
	})

	assert.NoError(t, err)
}

func TestKrakeBroker_Produce(t *testing.T) {
	pw, b := newInMemoryBroker()

	b.CreateTopic(TopicConfiguration{
		Name:            "my-topic",
		PartitionCount:  3,
		RetentionPeriod: 60 * time.Second,
	})

	msg := Message{
		Key:     nil,
		Message: []byte("foo"),
	}

	// when i produce a message to the broker
	err := b.Produce("my-topic", &msg)

	// the message is stored to disk
	assert.NoError(t, err)

	seg := pw.ActiveSegment(0)
	data, err := io.ReadAll(seg)
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(data))
}

func TestKrakeBroker_Consume(t *testing.T) {

}
