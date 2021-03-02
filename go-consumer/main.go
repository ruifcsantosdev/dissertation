package main

import (
	"context"
	"fmt"
	"github.com/lenaten/hl7"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"strings"
)

type Patient struct {
	PatitentId int    `hl7:"PID.1"`
	FirstName  string `hl7:"PID.5.1"`
	LastName   string `hl7:"PID.5.0"`
}

type Header struct {
	App       string `hl7:"MSH.3.0"`
	Facility  string `hl7:"MSH.4"`
	TimeStamp string `hl7:"MSH.7"`
	ControlId string `hl7:"MSH.9.2"`
	Type      string `hl7:"MSH.10"`
	Version   string `hl7:"MSH.12"`
}

type VisitInformation struct {
	Location    string `hl7:"PV1.3.2"`
	AccountType string `hl7:"PV1.2"`
}

type OrderObservation struct {
	ReportName string `hl7:"OBR.4.1"`
}

type OBXValue struct {
	ValueType                     string
	ObservationIdentifierId       string
	ObservationIdentifierText     string
	ObservationNameOfCodingSystem string
	ObservationSubID              string
	ObservationValue              string
	UnitIdentifier                string
	UnitText                      string
	UnitNameOfCodingSystem        string
	ObservationResultStatus       string
	ObxDate                       string
}

type Message struct {
	Patient          Patient
	Header           Header
	VisitInformation VisitInformation
	OrderObservation OrderObservation
	OBXValues        []OBXValue
}

func (msg *Message) AddObxValue(obxValue OBXValue) []OBXValue {
	msg.OBXValues = append(msg.OBXValues, obxValue)
	return msg.OBXValues
}

func GenerateMessage(message []byte) Message {
	msg := &hl7.Message{Value: message}
	msg.Parse()

	msg1 := Message{}
	msg.Unmarshal(&msg1.Patient)
	msg.Unmarshal(&msg1.Header)
	msg.Unmarshal(&msg1.VisitInformation)
	msg.Unmarshal(&msg1.OrderObservation)

	vals, err := msg.FindAll("OBX")
	if err != nil {
		panic(err)
	}

	for _, val := range vals {
		s := strings.Split(val, "|")

		sValue := strings.Split(s[3], "^")

		sUnit := strings.Split(s[6], "^")

		obx := OBXValue{
			ValueType:                     s[2],
			ObservationIdentifierId:       sValue[0],
			ObservationIdentifierText:     sValue[1],
			ObservationNameOfCodingSystem: sValue[2],
			ObservationSubID:              s[4],
			ObservationValue:              s[5],
			UnitIdentifier:                sUnit[0],
			UnitText:                      sUnit[1],
			UnitNameOfCodingSystem:        sUnit[2],
			ObservationResultStatus:       s[11],
			ObxDate:                       s[14],
		}
		msg1.AddObxValue(obx)
	}
	return msg1
}

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("kafkaBootstrapServers"),
		"group.id":          os.Getenv("kafkaConsumerGroupId"),
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	fmt.Println("Apache Kafka - Success connection")

	c.SubscribeTopics([]string{os.Getenv("kafkaHL7Topic")}, nil)

	clientOptions := options.Client().ApplyURI(os.Getenv("mongoDbApplyURI"))

	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("MongoDB - Success connection")

	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	collection := client.Database("Hl7MessagesDatabase").Collection("MessagesCollection")

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}else {
			msg := GenerateMessage(msg.Value)
			fmt.Println("New Message")
			res, err := collection.InsertOne(context.TODO(), msg)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Insert: ", res.InsertedID)
		}
	}
	c.Close()
}
