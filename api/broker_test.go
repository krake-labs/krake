package api

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Poll(timeout)
// Commit() => once processed, commit the offset
// Seek() => seek to an offset in a partition
// Unsubscribe() => unsub from a topic
// Pause() => stop consumption

func TestKrakeBroker_Produce_MemoryLayout(t *testing.T) {

}

func TestKrakeBroker_CreateDuplicateTopics(t *testing.T) {
	ws := bytes.NewBuffer([]byte{})
	b := NewKrakeBroker(ws)

	err := b.CreateTopic(topicConfiguration{
		Name:            "my-topic",
		PartitionCount:  3,
		RetentionPeriod: 2 * time.Hour,
	})

	assert.NoError(t, err)

	err = b.CreateTopic(topicConfiguration{
		Name:            "my-topic",
		PartitionCount:  3,
		RetentionPeriod: 2 * time.Hour,
	})

	assert.Equal(t, ErrTopicAlreadyExists, err)
}

func TestKrakeBroker_CreateTopic(t *testing.T) {
	ws := bytes.NewBuffer([]byte{})
	b := NewKrakeBroker(ws)

	err := b.CreateTopic(topicConfiguration{
		Name:            "my-topic",
		PartitionCount:  3,
		RetentionPeriod: 2 * time.Hour,
	})

	assert.NoError(t, err)
}

func TestKrakeBroker_Produce(t *testing.T) {
	// given a broker
	ws := bytes.NewBuffer([]byte{})
	b := NewKrakeBroker(ws)

	msg := Message{
		Key:     nil,
		Message: []byte("foo"),
	}

	// when i produce a message to the broker
	err := b.Produce("my-topic", &msg)

	// the message is stored to disk
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(ws.Bytes()))
}

func TestKrakeBroker_Consume(t *testing.T) {

}
