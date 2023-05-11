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

// TODO test where segment is capped to smaller size
// and we have writes on different active segments

func TestKrakeBroker_Produce_MultipleTopics(t *testing.T) {
	pw, b := newInMemoryBroker()

	PartitionCount := 2

	b.CreateTopic(TopicConfiguration{
		Name:            "topic-a",
		PartitionCount:  PartitionCount,
		RetentionPeriod: 24 * time.Hour,
	})

	b.CreateTopic(TopicConfiguration{
		Name:            "topic-b",
		PartitionCount:  PartitionCount,
		RetentionPeriod: 24 * time.Hour,
	})

	b.Produce("topic-a", &Message{
		Key:     nil,
		Message: []byte("hello"),
	})

	b.Produce("topic-a", &Message{
		Key:     nil,
		Message: []byte("world"),
	})

	b.Produce("topic-b", &Message{
		Key:     nil,
		Message: []byte("world"),
	})

	// assumes the active segment contains our writes
	segment := pw.ActiveSegment(TopicPartitionKey{"topic-a", 0})
	data, _ := io.ReadAll(segment)
	assert.Equal(t, "hello", string(data))

	segment = pw.ActiveSegment(TopicPartitionKey{"topic-a", 1})
	data, _ = io.ReadAll(segment)
	assert.Equal(t, "world", string(data))

	segment = pw.ActiveSegment(TopicPartitionKey{"topic-b", 0})
	data, _ = io.ReadAll(segment)
	assert.Equal(t, "world", string(data))
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
		seg :=
			pw.ActiveSegment(TopicPartitionKey{"my-topic", int32(i)})
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

	seg := pw.ActiveSegment(TopicPartitionKey{"my-topic", 0})
	data, err := io.ReadAll(seg)
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(data))
}

func TestKrakeBroker_Produce_NoTopicExists(t *testing.T) {
	_, b := newInMemoryBroker()

	msg := Message{
		Key:     nil,
		Message: []byte("foo"),
	}

	// when i produce a message to the broker
	err := b.Produce("my-topic", &msg)

	assert.ErrorIs(t, err, ErrNoSuchTopic)
}

func TestKrakeBroker_Consume(t *testing.T) {

}
