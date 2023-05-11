package api

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"time"
)

type Broker interface {
	Produce(s string, msg *Message) error
	CreateTopic(configuration TopicConfiguration) error
	Configure(m map[string]interface{})
}

type TopicPartitionKey struct {
	Topic          string
	PartitionIndex int32
}

type FilePool struct {
	// partition index -> file
	data map[TopicPartitionKey]*os.File
}

// Open creates a new segment
func (f FilePool) Open(segSize int, fileName string) *os.File {
	// FIXME(FELIX): /tmp/ dir should be taken from config.

	temp, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	if err = temp.Truncate(int64(segSize)); err != nil {
		temp.Close()
		panic(err)
	}
	return temp
	//return internal.NewMemoryFile(segSize)
}

type PartitionWriter struct {
	filePool *FilePool
}

func NewPartitionWriter() *PartitionWriter {
	return &PartitionWriter{
		filePool: &FilePool{
			data: map[TopicPartitionKey]*os.File{},
		},
	}
}

func (pw *PartitionWriter) loadSegment(key TopicPartitionKey, offs int64) (*os.File, error) {
	k := fmt.Sprintf("/tmp/krake/%s-%d.%d.log", key.Topic, offs, key.PartitionIndex)
	file, _ := os.Open(k)
	return file, nil
}

func (pw *PartitionWriter) ActiveSegment(key TopicPartitionKey) (*os.File, error) {
	seg, ok := pw.filePool.data[key]
	if !ok {
		return nil, errors.New("no such segment")
	}
	return seg, nil
}

type TopicConfiguration struct {
	Name            string
	PartitionCount  int
	RetentionPeriod time.Duration
}

type KrakeBroker struct {
	*PartitionWriter

	topics map[string]TopicConfiguration

	// used for round-robin partitioning
	currPartitionIndex int32

	Config map[string]interface{}
}

func NewKrakeBroker(writeStrategy *PartitionWriter) *KrakeBroker {
	return &KrakeBroker{
		PartitionWriter: writeStrategy,
		topics:          map[string]TopicConfiguration{},
	}
}

func (k *KrakeBroker) Configure(m map[string]interface{}) {
	// FIXME(FELIX): overwrite configurations with the values
	// or append?
	k.Config = m
}

func (k *KrakeBroker) partitionIndex(key []byte, partitionCount int) int32 {
	if len(key) == 0 {
		curr := k.currPartitionIndex
		k.currPartitionIndex++
		if k.currPartitionIndex >= int32(partitionCount) {
			k.currPartitionIndex = 0
		}
		return curr
	}

	hash := fnv.New32a()
	_, err := hash.Write(key)
	if err != nil {
		log.Println("failed to compute partition index")
		return -1
	}
	return int32(hash.Sum32())
}

var (
	ErrWriteFailed        = errors.New("failed to write bytes")
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrNoSuchTopic        = errors.New("no such topic")
)

func (k *KrakeBroker) Produce(topic string, msg *Message) error {
	topicCfg, ok := k.topics[topic]
	if !ok {
		return ErrNoSuchTopic
	}

	partitionIdx := k.partitionIndex(msg.Key, topicCfg.PartitionCount)
	log.Println("partition index", partitionIdx)

	if partitionIdx == -1 {
		panic("unhandled error")
	}

	segSize, ok := k.Config["log.segment.bytes"].(int)
	if !ok {
		segSize = 1_000_000 // 1MiB
	}

	toWrite := len(msg.Message)
	for toWrite != 0 {
		// for now this only handles writing to the latest segment in the partition
		key := TopicPartitionKey{
			Topic:          topic,
			PartitionIndex: partitionIdx,
		}

		// cases:
		// 1. active segment is nul because we've just started the app
		// if so we have to find it.
		// 2. we have no active segment at all
		seg, err := k.ActiveSegment(key)
		if err != nil {
			baseOffs := 0 // for case 1 this changes.
			seg = k.openNewSegment(segSize, baseOffs, key)
		}

		// calc remaining space
		fileInfo, err := seg.Stat()
		if err != nil {
			panic(err)
		}

		currentPosition, err := seg.Seek(0, io.SeekCurrent)
		if err != nil {
			fmt.Println("Error getting current position in file:", err)
		}

		bytesLeft := fileInfo.Size() - currentPosition

		log.Println(toWrite, "...", bytesLeft)

		if int64(toWrite) >= bytesLeft {
			// FIXME kafka will write to a segment
			// until we exceed the maximum file size for an OS
			// we don't allow for messages over than 1MB so this should be fine
			// and an edge case that is not often encountered. that said
			// we should consider a safeguard for this.
			writtenBytes, err := seg.Write(msg.Message)
			if err != nil {
				panic(err)
			}
			toWrite -= writtenBytes

			seg.Close()

			// FIXME baseOffs is wrong here
			seg = k.openNewSegment(segSize, writtenBytes, key)
		} else {
			log.Println("plenty of space writing whole thang")
			seg.Write(msg.Message)
			toWrite -= len(msg.Message)
		}
	}

	return nil
}

func (k *KrakeBroker) openNewSegment(segSize int, baseOffs int, key TopicPartitionKey) *os.File {
	path := fmt.Sprintf("/tmp/krake/%s-%d.%d.log", key.Topic, baseOffs, key.PartitionIndex)
	log.Println("opening a new segment file", path)
	f := k.filePool.Open(segSize, path)
	k.filePool.data[key] = f
	return f
}

func (k *KrakeBroker) CreateTopic(cfg TopicConfiguration) error {
	if _, ok := k.topics[cfg.Name]; ok {
		return ErrTopicAlreadyExists
	}
	k.topics[cfg.Name] = cfg
	return nil
}
