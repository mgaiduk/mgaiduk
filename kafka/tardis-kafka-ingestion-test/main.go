package main

import (
	"context"
	"fmt"
	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"time"
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
	TestChatroomGiftEvents(kafkaClient)
}ßß

func TestChatroomGiftEvents(kafkaClient *Kafka) {
	topic := "live-chatroomgiftevents"

	chatroomGiftCodec := getAvroCodecBySchemaPath("schema/sc-chatroomgiftevents.avsc")
	row1 := map[string]interface{}{
		"chatroomId": goavro.Union("string", "1"),
		"senderId":   goavro.Union("string", "2"),
		"receiverId": goavro.Union("string", "3"),
		"coinsSpent": goavro.Union("long", 10),
		"time":       goavro.Union("long", time.Now().UnixMilli()),
	}

	fmt.Println(time.Now().Unix())

	binary, err := chatroomGiftCodec.BinaryFromNative(nil, row1)
	if err != nil {
		fmt.Println("error while converting native data to avro binary", map[string]interface{}{
			"err":  err.Error(),
			"data": row1,
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
			"data": row1,
		})
	}
}

//func TestCommentEvent(kafkaClient *Kafka) {
//	topic := "moj-livestream-commentEvent"
//
//	liveCreatorGradeCodec := getAvroCodecBySchemaPath("schema/livestreamCommentEvent.avsc")
//	row1 := map[string]interface{}{
//		"livestreamId": goavro.Union("string", "livestreamId1"),
//		"authorId":     goavro.Union("string", "authorId2"),
//		"hostId":       goavro.Union("string", "hostId1"),
//		"time":         goavro.Union("long", time.Now().UnixMilli()),
//	}
//	binary, err := liveCreatorGradeCodec.BinaryFromNative(nil, row1)
//	if err != nil {
//		fmt.Println("error while converting native data to avro binary", map[string]interface{}{
//			"err":  err.Error(),
//			"data": row1,
//		})
//	}
//	err = kafkaClient.BatchPublish([]kafka.Message{
//		{
//			Topic: topic,
//			Value: binary,
//		},
//	})
//	if err != nil {
//		fmt.Println("error while publishing message to kafka", map[string]interface{}{
//			"err":  err.Error(),
//			"data": row1,
//		})
//	}
//}

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
