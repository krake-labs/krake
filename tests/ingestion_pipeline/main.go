package main

import (
	"github.com/bat-labs/krake/api"
)

func main() {
	pw := api.NewPartitionWriter()
	b := api.NewKrakeBroker(pw)

	b.Configure(map[string]interface{}{
		"log.dirs":          "/tmp/",
		"log.segment.bytes": 100,
	})

	b.CreateTopic(api.TopicConfiguration{
		Name:            "",
		PartitionCount:  0,
		RetentionPeriod: 0,
	})
}
