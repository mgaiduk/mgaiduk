package main

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
)

func NewKafkaClient() *Kafka {
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		AllowAutoTopicCreation: false,
		RequiredAcks:           kafka.RequireOne,
		MaxAttempts:            5,
		BatchSize:              1,
		Balancer:               kafka.Murmur2Balancer{},
		//BatchTimeout: time.Millisecond * 10,
	}
	return &Kafka{
		kafkaWriter: kafkaWriter,
	}
}

type Kafka struct {
	kafkaWriter *kafka.Writer
}

func (kf *Kafka) BatchPublish(messages []kafka.Message) error {
	fmt.Println(messages)

	return kf.kafkaWriter.WriteMessages(context.Background(), messages...)
}

func main() {
	kafkaClient := NewKafkaClient()
	PublishToKafka(map[string]interface{}{
		"livestreamId":    goavro.Union("string", "livestreamId1"),
		"hostId":          goavro.Union("string", "hostId1"),
		"memberId":        goavro.Union("string", "memberId1"),
		"shortPressCount": goavro.Union("int", 1),
		"longPressCount":  goavro.Union("int", 2),
		"time":            goavro.Union("long", 1683116230000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-likeEvent", "schema/moj-livestream-likeEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId":    goavro.Union("string", "livestreamId1"),
		"hostId":          goavro.Union("string", "hostId1"),
		"memberId":        goavro.Union("string", "memberId1"),
		"shortPressCount": goavro.Union("int", 4),
		"longPressCount":  goavro.Union("int", 6),
		"time":            goavro.Union("long", 1683116530000), // Wed  3 May 2023 13:22:10 BST
	}, "moj-livestream-likeEvent", "schema/moj-livestream-likeEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId":    goavro.Union("string", "livestreamId2"),
		"hostId":          goavro.Union("string", "hostId1"),
		"memberId":        goavro.Union("string", "memberId1"),
		"shortPressCount": goavro.Union("int", 1),
		"longPressCount":  goavro.Union("int", 1),
		"time":            goavro.Union("long", 1683116830000), // Wed  3 May 2023 13:27:10 BST
	}, "moj-livestream-likeEvent", "schema/moj-livestream-likeEvent.avsc", kafkaClient)
}

func PublishToKafka(msg map[string]interface{}, topic, schemaPath string, kafkaClient *Kafka) {
	codec := getAvroCodecBySchemaPath(schemaPath)
	binary, err := codec.BinaryFromNative(nil, msg)
	if err != nil {
		fmt.Println("error while converting native data to avro binary", map[string]interface{}{
			"err":  err.Error(),
			"data": msg,
		})
	}
	err = kafkaClient.BatchPublish([]kafka.Message{
		{
			Topic: topic,
			Value: binary,
		},
	})
	if err != nil {
		fmt.Println("error while publishing message to kafka", map[string]interface{}{
			"err":  err.Error(),
			"data": msg,
		})
	}
}

func getAvroCodecBySchemaPath(filename string) *goavro.Codec {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("error in reading avro schema file", map[string]interface{}{
			"err": err.Error(),
		})
		return nil
	}
	avroSchema := string(data)
	avroCodec, err := goavro.NewCodecForStandardJSON(avroSchema)
	if err != nil {
		fmt.Println("error in loading the Avro schmea", map[string]interface{}{
			"err":      err.Error(),
			"fileName": filename,
		})
		return nil
	}
	return avroCodec
}
