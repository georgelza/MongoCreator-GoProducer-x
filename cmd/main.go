/*****************************************************************************
*
*	File			: producer.go
*
* 	Created			: 4 Dec 2023
*
*	Description		: Golang Fake data producer, part of the MongoCreator project.
*					: We will create a sales basket of items as a document, to be posted onto Confluent Kafka topic, we will then
*					: then seperately post a payment onto a seperate topic. Both topics will then be sinked into MongoAtlas collections
*					: ... EXPAND ...
*
*	Modified		: 4 Dec 2023	- Start
*					: Created the main fake document creator functions and framework, including the fake seed data to be used.
*
*					: 5 Dec 2023
*					: Added code to output/save the created documents to the output_path directory specified
*					: Added code to push the documents onto Confluent Kafka cluster hosted topics
*					: Uploaded project to Git Repo
*
*					: 6 Dec 2023
*					: introduced some code that when testsize is set to 0 then it uses <LARGE> value to imply run continiously.
*					: Modified the save to file to save all basket docs to one file per run and all payments docs to a single file.
*					: made the time off set a configuration file value (TimeOffset)
*					: made the max items per basket a configuration file value ()
*					: made the quantity per product a configuration file value ()
*
*					: 24 Jan 2024
*					: going to add code/module to disable the Kafka push of docs and rather directly push onto Mongo Atlas, thus
*					: bypassing the Confluent Kakfa cluster avaiability challenge.
*
*					: 15 Jun 2025
*					: Moved create of the payment out into constructPayments()
*					: changed types.Tp_payment as a normal struct into types.Pb_Payment => protobuf
*
*					: 16 June 2025
*					: changed types.Tp_basket as a normal struct into types.Pb_Basket => protobuf
*					: Schema registries created as per .proto files in the types/ directory
*					: protoc --proto_path=. --go_out=. record.proto
*
*					: 17 June 2024
*					: Refactored producer following :
*					: https://medium.com/@ninucium/is-using-kafka-with-schema-registry-and-protobuf-worth-it-part-1-1c4a9995a5d3
*
*	Git				: https://github.com/georgelza/MongoCreator-GoProducer
*
*	By				: George Leonard (georgelza@gmail.com) aka georgelza on Discord and Mongo Community Forum
*
*	jsonformatter 	: https://jsonformatter.curiousconcept.com/#
*
*****************************************************************************/

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/TylerBrock/colorjson"
	"github.com/brianvoe/gofakeit"
	"github.com/google/uuid"
	"github.com/tkanos/gonfig"

	// My Types/Structs/functions
	"cmd/internal/kafka"
	"cmd/types"

	cpkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	glog "google.golang.org/grpc/grpclog"

	// Filter JSON array
	// MongoDB
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	grpcLog  glog.LoggerV2
	varSeed  types.TPSeed
	vGeneral types.Tp_general
	pathSep  = string(os.PathSeparator)
	runId    string
	vKafka   types.TKafka
	vMongodb types.TMongodb
	producer kafka.SRProducer
)

func init() {

	// Keeping it very simple
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

	grpcLog.Infoln("###############################################################")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Project   : GoProducer 2.0")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Comment   : MongoCreator Project and lots of Kafka")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   By        : George Leonard (georgelza@gmail.com)")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Date/Time :", time.Now().Format("2006-01-02 15:04:05"))
	grpcLog.Infoln("#")
	grpcLog.Infoln("###############################################################")
	grpcLog.Infoln("")
	grpcLog.Infoln("")

}

func loadConfig(params ...string) types.Tp_general {

	var err error

	vGeneral := types.Tp_general{}
	env := "dev"
	if len(params) > 0 { // Input environment was specified, so lets use it
		env = params[0]
		grpcLog.Info("*")
		grpcLog.Info("* Called with Argument => ", env)
		grpcLog.Info("*")

	}

	vGeneral.CurrentPath, err = os.Getwd()
	if err != nil {
		grpcLog.Fatalln("Problem retrieving current path: %s", err)

	}

	vGeneral.OSName = runtime.GOOS

	// General config file
	fileName := fmt.Sprintf("%s%s%s_app.json", vGeneral.CurrentPath, pathSep, env)
	err = gonfig.GetConf(fileName, &vGeneral)
	if err != nil {
		grpcLog.Fatalln("Error Reading Config File: ", err)

	} else {

		vHostname, err := os.Hostname()
		if err != nil {
			grpcLog.Fatalln("Can't retrieve hostname %s", err)

		}
		vGeneral.Hostname = vHostname
		vGeneral.SeedFile = fmt.Sprintf("%s%s%s", vGeneral.CurrentPath, pathSep, vGeneral.SeedFile)

	}

	if vGeneral.Json_to_file == 1 {
		vGeneral.Output_path = fmt.Sprintf("%s%s%s", vGeneral.CurrentPath, pathSep, vGeneral.Output_path)
	}

	if vGeneral.EchoConfig == 1 {
		grpcLog.Infoln("*")
		grpcLog.Infoln("* Config:")
		grpcLog.Infoln("* Current path:", vGeneral.CurrentPath)
		grpcLog.Infoln("* Config File :", fileName)
		grpcLog.Infoln("*")

		printConfig(vGeneral)
	}

	return vGeneral
}

// Load Kafka specific configuration Parameters, this is so that we can gitignore this dev_kafka.json file/seperate
// from the dev_app.json file
func loadKafka(params ...string) types.TKafka {

	vKafka := types.TKafka{}
	env := "dev"
	if len(params) > 0 {
		env = params[0]
	}

	path, err := os.Getwd()
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Problem retrieving current path: %s", err))
		os.Exit(1)

	}

	//	fileName := fmt.Sprintf("%s/%s_app.json", path, env)
	fileName := fmt.Sprintf("%s/%s_kafka.json", path, env)
	err = gonfig.GetConf(fileName, &vKafka)
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Error Reading Kafka File: %s", err))
		os.Exit(1)

	}

	vGeneral.KafkaConfigFile = fileName

	if vGeneral.Debuglevel > 0 {

		grpcLog.Info("*")
		grpcLog.Info("* Kafka Config :")
		grpcLog.Info(fmt.Sprintf("* Current path : %s", path))
		grpcLog.Info(fmt.Sprintf("* Kafka File   : %s", fileName))
		grpcLog.Info("*")

	}

	vKafka.Sasl_password = os.Getenv("Sasl_password")
	vKafka.Sasl_username = os.Getenv("Sasl_username")

	if vGeneral.EchoConfig == 1 {
		printKafkaConfig(vKafka)
	}

	return vKafka
}

func loadMongoProps(params ...string) types.TMongodb {

	vMongodb := types.TMongodb{}
	env := "dev"
	if len(params) > 0 {
		env = params[0]
	}

	path, err := os.Getwd()
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Problem retrieving current path: %s", err))
		os.Exit(1)

	}

	//	fileName := fmt.Sprintf("%s/%s_app.json", path, env)
	fileName := fmt.Sprintf("%s/%s_mongo.json", path, env)
	err = gonfig.GetConf(fileName, &vMongodb)
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Error Reading Mongo File: %s", err))
		os.Exit(1)

	}

	vGeneral.MongoConfigFile = fileName

	if vGeneral.Debuglevel > 0 {

		grpcLog.Info("*")
		grpcLog.Info("* Mongo Config :")
		grpcLog.Info(fmt.Sprintf("* Current path : %s", path))
		grpcLog.Info(fmt.Sprintf("* Mongo File   : %s", fileName))
		grpcLog.Info("*")

	}

	vMongodb.Username = os.Getenv("mongo_username")
	vMongodb.Password = os.Getenv("mongo_password")

	if vMongodb.Username != "" {

		vMongodb.Uri = fmt.Sprintf("%s://%s:%s@%s&w=majority", vMongodb.Root, vMongodb.Username, vMongodb.Password, vMongodb.Url)

	} else {

		vMongodb.Uri = fmt.Sprintf("%s://%s&w=majority", vMongodb.Root, vMongodb.Url)
	}

	if vGeneral.EchoConfig == 1 {
		printMongoConfig(vMongodb)
	}

	return vMongodb
}

func loadSeed(fileName string) types.TPSeed {

	var vSeed types.TPSeed

	err := gonfig.GetConf(fileName, &vSeed)
	if err != nil {
		grpcLog.Fatalln("Error Reading Seed File: ", err)

	}

	v, err := json.Marshal(vSeed)
	if err != nil {
		grpcLog.Fatalln("Marchalling error: ", err)
	}

	if vGeneral.EchoSeed == 1 {
		prettyJSON(string(v))

	}

	if vGeneral.Debuglevel > 0 {
		grpcLog.Infoln("*")
		grpcLog.Infoln("* Seed :")
		grpcLog.Infoln("* Current path:", vGeneral.CurrentPath)
		grpcLog.Infoln("* Seed File   :", vGeneral.SeedFile)
		grpcLog.Infoln("*")

	}

	return vSeed
}

func printConfig(vGeneral types.Tp_general) {

	grpcLog.Info("****** General Parameters *****")
	grpcLog.Info("*")
	grpcLog.Info("* Hostname is\t\t\t", vGeneral.Hostname)
	grpcLog.Info("* OS is \t\t\t", vGeneral.OSName)
	grpcLog.Info("*")
	grpcLog.Info("* Debug Level is\t\t", vGeneral.Debuglevel)
	grpcLog.Info("*")
	grpcLog.Info("* Sleep Duration is\t\t", vGeneral.Sleep)
	grpcLog.Info("* Test Batch Size is\t\t", vGeneral.Testsize)
	grpcLog.Info("* Echo Seed is\t\t", vGeneral.EchoSeed)
	grpcLog.Info("* Seed File is\t\t", vGeneral.SeedFile)
	grpcLog.Info("* Json to File is\t\t", vGeneral.Json_to_file)
	if vGeneral.Json_to_file == 1 {
		grpcLog.Infoln("* Output path\t\t\t", vGeneral.Output_path)
	}
	grpcLog.Info("* Kafka Enabled is\t\t", vGeneral.KafkaEnabled)
	grpcLog.Info("* Mongo Enabled is\t\t", vGeneral.MongoAtlasEnabled)

	grpcLog.Info("*")
	grpcLog.Info("*******************************")

	grpcLog.Info("")

}

// print some more configurations
func printKafkaConfig(vKafka types.TKafka) {

	fmt.Printf("xxxxxxxxxxxxxxxxxxx")

	grpcLog.Info("****** Kafka Connection Parameters *****")
	grpcLog.Info("*")
	grpcLog.Info("* Kafka bootstrap Server is\t", vKafka.Bootstrapservers)
	grpcLog.Info("* Kafka schema Registry is\t", vKafka.SchemaRegistryURL)
	grpcLog.Info("* Kafka Basket Topic is\t", vKafka.BasketTopicname)
	grpcLog.Info("* Kafka Payment Topic is\t", vKafka.PaymentTopicname)
	grpcLog.Info("* Kafka # Parts is\t\t", vKafka.Numpartitions)
	grpcLog.Info("* Kafka Rep Factor is\t\t", vKafka.Replicationfactor)
	grpcLog.Info("* Kafka Retension is\t\t", vKafka.Retension)
	grpcLog.Info("* Kafka ParseDuration is\t", vKafka.Parseduration)

	grpcLog.Info("* Kafka SASL Mechanism is\t", vKafka.Sasl_mechanisms)
	grpcLog.Info("* Kafka SASL Username is\t", vKafka.Sasl_username)

	grpcLog.Info("*")
	grpcLog.Info("* Kafka Flush Size is\t\t", vKafka.Flush_interval)
	grpcLog.Info("*")
	grpcLog.Info("*******************************")

	grpcLog.Info("")

}

// print some more configurations
func printMongoConfig(vMongodb types.TMongodb) {

	grpcLog.Info("*")
	grpcLog.Info("****** MongoDB Connection Parameters *****")
	grpcLog.Info("*")

	grpcLog.Info("* Mongo URL is\t\t", vMongodb.Url)
	grpcLog.Info("* Mongo Port is\t\t", vMongodb.Port)
	grpcLog.Info("* Mongo DataStore is\t\t", vMongodb.Datastore)
	grpcLog.Info("* Mongo Username is\t\t", vMongodb.Username)
	grpcLog.Info("* Mongo Basket Collection is\t", vMongodb.Basketcollection)
	grpcLog.Info("* Mongo Payment Collection is\t", vMongodb.Paymentcollection)
	grpcLog.Info("* Mongo Batch szie is\t\t", vMongodb.Batch_size)

	grpcLog.Info("*")
	grpcLog.Info("*******************************")

	grpcLog.Info("")

}

// Create Kafka topic if not exist, using admin client
func CreateTopic(props types.TKafka) {

	// we using kafka aliased to cpkafka as we've created our own kafka class located in internal/kafka
	cm := cpkafka.ConfigMap{
		"bootstrap.servers":       props.Bootstrapservers,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
	}

	if props.Sasl_mechanisms != "" {
		cm["sasl.mechanisms"] = props.Sasl_mechanisms
		cm["security.protocol"] = props.Security_protocol
		cm["sasl.username"] = props.Sasl_username
		cm["sasl.password"] = props.Sasl_password

		if vGeneral.Debuglevel > 0 {
			grpcLog.Info("* Security Authentifaction configured in ConfigMap")

		}
	}

	if vGeneral.Debuglevel > 0 {
		grpcLog.Info("* Basic Client ConfigMap compiled")
	}

	adminClient, err := cpkafka.NewAdminClient(&cm)
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Admin Client Creation Failed: %s", err))
		os.Exit(1)

	}

	if vGeneral.Debuglevel > 0 {
		grpcLog.Info("* Admin Client Created Succeeded")

	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDuration, err := time.ParseDuration(props.Parseduration)
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Error Configuring maxDuration via ParseDuration: %s", props.Parseduration))
		os.Exit(1)

	}

	if vGeneral.Debuglevel > 0 {
		grpcLog.Info("* Configured maxDuration via ParseDuration")

	}

	// the topics should rather be passed in as a array... but we want to be specific - rather move this external
	// and call this routine twice from a loop.
	//

	// Basket topic
	results, err := adminClient.CreateTopics(ctx,
		[]cpkafka.TopicSpecification{{
			Topic:             props.BasketTopicname,
			NumPartitions:     props.Numpartitions,
			ReplicationFactor: props.Replicationfactor}},
		cpkafka.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		grpcLog.Error(fmt.Sprintf("Problem during the topic creation: %v", err))
		os.Exit(1)
	}

	// Check for specific topic errors
	for _, result := range results {
		if result.Error.Code() != cpkafka.ErrNoError &&
			result.Error.Code() != cpkafka.ErrTopicAlreadyExists {
			grpcLog.Error(fmt.Sprintf("Topic Creation Failed for %s: %v", result.Topic, result.Error.String()))
			os.Exit(1)

		} else {
			if vGeneral.Debuglevel > 0 {
				grpcLog.Info(fmt.Sprintf("* Topic Creation Succeeded for %s", result.Topic))

			}
		}
	}

	// Payment topic
	results, err = adminClient.CreateTopics(ctx,
		[]cpkafka.TopicSpecification{{
			Topic:             props.PaymentTopicname,
			NumPartitions:     props.Numpartitions,
			ReplicationFactor: props.Replicationfactor}},
		cpkafka.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		grpcLog.Error(fmt.Sprintf("Problem during the topic creation: %v", err))
		os.Exit(1)
	}

	// Check for specific topic errors
	for _, result := range results {
		if result.Error.Code() != cpkafka.ErrNoError &&
			result.Error.Code() != cpkafka.ErrTopicAlreadyExists {
			grpcLog.Error(fmt.Sprintf("Topic Creation Failed for %s: %v", result.Topic, result.Error.String()))
			os.Exit(1)

		} else {
			if vGeneral.Debuglevel > 0 {
				grpcLog.Info(fmt.Sprintf("* Topic Creation Succeeded for %s", result.Topic))

			}
		}
	}

	adminClient.Close()
	grpcLog.Info("")

}

// Helper Functions
// Pretty Print JSON string
func prettyJSON(ms string) {

	var obj map[string]interface{}

	json.Unmarshal([]byte(ms), &obj)

	// Make a custom formatter with indent set
	f := colorjson.NewFormatter()
	f.Indent = 4

	// Marshall the Colorized JSON
	result, _ := f.Marshal(obj)
	fmt.Println(string(result))

}

func xprettyJSON(ms string) string {

	var obj map[string]interface{}

	json.Unmarshal([]byte(ms), &obj)

	// Make a custom formatter with indent set
	f := colorjson.NewFormatter()
	f.Indent = 4

	// Marshall the Colorized JSON
	result, _ := f.Marshal(obj)
	//fmt.Println(string(result))
	return string(result)
}

func JsonToBson(message []byte) ([]byte, error) {
	reader, err := bsonrw.NewExtJSONValueReader(bytes.NewReader(message), true)
	if err != nil {
		return []byte{}, err
	}
	buf := &bytes.Buffer{}
	writer, _ := bsonrw.NewBSONValueWriter(buf)
	err = bsonrw.Copier{}.CopyDocument(writer, reader)
	if err != nil {
		return []byte{}, err
	}
	marshaled := buf.Bytes()
	return marshaled, nil
}

// https://stackoverflow.com/questions/18390266/how-can-we-truncate-float64-type-to-a-particular-precision
func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}
func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func constructFakeBasket() (pb_Basket types.Pb_Basket, eventTimestamp time.Time, storeName string, err error) {

	// Fake Data etc, not used much here though
	// https://github.com/brianvoe/gofakeit
	// https://pkg.go.dev/github.com/brianvoe/gofakeit

	gofakeit.Seed(0)

	var store types.Idstruct
	var clerk types.Idstruct
	if vGeneral.Store == 0 {
		// Determine how many Stores we have in seed file,
		// and build the 2 structures from that viewpoint
		storeCount := len(varSeed.Stores) - 1
		nStoreId := gofakeit.Number(0, storeCount)
		store.Id = varSeed.Stores[nStoreId].Id
		store.Name = varSeed.Stores[nStoreId].Name

	} else {
		// We specified a specific store
		store.Id = varSeed.Stores[vGeneral.Store].Id
		store.Name = varSeed.Stores[vGeneral.Store].Name

	}

	// Determine how many Clerks we have in seed file,
	clerkCount := len(varSeed.Clerks) - 1
	nClerkId := gofakeit.Number(0, clerkCount)
	clerk.Id = varSeed.Clerks[nClerkId].Id
	clerk.Name = varSeed.Clerks[nClerkId].Name

	// Uniqiue reference to the basket/sale
	txnId := uuid.New().String()

	// time that everything happened, the 1st as a Unix Epoc time representation,
	// the 2nd in nice human readable milli second representation.
	eventTimestamp = time.Now()
	eventTime := eventTimestamp.Format("2006-01-02T15:04:05.000") + vGeneral.TimeOffset

	// How many potential products do we have
	productCount := len(varSeed.Products) - 1
	// now pick from array a random products to add to basket, by using 1 as a start point we ensure we always have at least 1 item.
	nBasketItems := gofakeit.Number(1, vGeneral.Max_items_basket)

	nett_amount := 0.0

	var BasketItems []*types.BasketItem

	for count := 0; count < nBasketItems; count++ {

		productId := gofakeit.Number(0, productCount)

		quantity := gofakeit.Number(1, vGeneral.Max_quantity)
		price := varSeed.Products[productId].Price

		BasketItem := &types.BasketItem{
			Id:       varSeed.Products[productId].Id,
			Name:     varSeed.Products[productId].Name,
			Brand:    varSeed.Products[productId].Brand,
			Category: varSeed.Products[productId].Category,
			Price:    varSeed.Products[productId].Price,
			Quantity: int32(quantity),
		}
		BasketItems = append(BasketItems, BasketItem)

		nett_amount = nett_amount + price*float64(quantity)

	}

	nett_amount = toFixed(nett_amount, 2)
	vat_amount := toFixed(nett_amount*vGeneral.Vatrate, 2) // sales tax
	total_amount := toFixed(nett_amount+vat_amount, 2)
	terminalPoint := gofakeit.Number(0, 20)

	pb_Basket = types.Pb_Basket{
		InvoiceNumber: txnId,
		SaleDateTime:  eventTime,
		SaleTimestamp: fmt.Sprint(eventTimestamp.UnixMilli()),
		Store:         &store,
		Clerk:         &clerk,
		TerminalPoint: strconv.Itoa(terminalPoint),
		BasketItems:   BasketItems,
		Nett:          nett_amount,
		Vat:           vat_amount,
		Total:         total_amount,
	}

	return pb_Basket, eventTimestamp, store.Name, nil
}

func constructPayments(txnId string, eventTimestamp time.Time, total_amount float64) (pb_Payment types.Pb_Payment) {

	// We're saying payment can be now up to 5min and 59 seconds later
	payTimestamp := eventTimestamp.Local().Add(time.Minute*time.Duration(gofakeit.Number(0, 5)) + time.Second*time.Duration(gofakeit.Number(0, 59)))
	payTime := payTimestamp.Format("2006-01-02T15:04:05.000") + vGeneral.TimeOffset

	pb_Payment = types.Pb_Payment{
		InvoiceNumber:    txnId,
		PayDateTime:      payTime,
		PayTimestamp:     fmt.Sprint(payTimestamp.UnixMilli()),
		Paid:             total_amount,
		FinTransactionID: uuid.New().String(),
	}

	return pb_Payment
}

// Big worker... This is where all the magic is called from, ha ha.
func runLoader(arg string) {

	var err error
	var f_basket *os.File
	var f_pmnt *os.File
	var basketcol *mongo.Collection
	var paymentcol *mongo.Collection

	// Initialize the vGeneral struct variable - This holds our configuration settings.
	vGeneral = loadConfig(arg)

	// Lets get Seed Data from the specified seed file
	varSeed = loadSeed(vGeneral.SeedFile)

	// Initiale the vKafka struct variable - This holds our Confluent Kafka configuration settings.
	// if Kafka is enabled then create the confluent kafka connection session/objects
	if vGeneral.KafkaEnabled == 1 {

		vKafka = loadKafka(arg)

		fmt.Println("Mechanism", vKafka.Sasl_mechanisms)
		fmt.Println("Username", vKafka.Sasl_username)
		fmt.Println("Password", vKafka.Sasl_password)
		fmt.Println("Broker", vKafka.Bootstrapservers)

		// Lets make sure the topic/s exist
		CreateTopic(vKafka)

		// --
		// Create Producer instance
		// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewProducer

		if vGeneral.Debuglevel > 0 {
			grpcLog.Info("**** Configure Client Kafka Connection ****")
			grpcLog.Info("*")
			grpcLog.Info(fmt.Sprintf("* Kafka bootstrap Server is %s", vKafka.Bootstrapservers))
			if vKafka.SchemaRegistryURL != "" {
				grpcLog.Info(fmt.Sprintf("* Schema Registry URL is    %s", vKafka.SchemaRegistryURL))
			}
		}

		cm := cpkafka.ConfigMap{
			"bootstrap.servers":       vKafka.Bootstrapservers,
			"broker.version.fallback": "0.10.0.0",
			"api.version.fallback.ms": 0,
			"client.id":               vGeneral.Hostname,
		}

		if vGeneral.Debuglevel > 0 {
			grpcLog.Info("* Basic Client ConfigMap compiled")

		}

		if vKafka.Sasl_mechanisms != "" {
			cm["sasl.mechanisms"] = vKafka.Sasl_mechanisms
			cm["security.protocol"] = vKafka.Security_protocol
			cm["sasl.username"] = vKafka.Sasl_username
			cm["sasl.password"] = vKafka.Sasl_password
			if vGeneral.Debuglevel > 0 {
				grpcLog.Info("* Security Authentifaction configured in ConfigMap")

			}
		}

		// internal/kafka/producer.go
		producer, err := kafka.NewProducer(cm, vKafka.SchemaRegistryURL)
		defer producer.Close()

		// Check for errors in creating the Producer
		if err != nil {
			grpcLog.Error(fmt.Sprintf("😢Oh noes, there's an error creating the Producer! %s", err))

			if ke, ok := err.(cpkafka.Error); ok {
				switch ec := ke.Code(); ec {
				case cpkafka.ErrInvalidArg:
					grpcLog.Error(fmt.Sprintf("😢 Can't create the producer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, err))
				default:
					grpcLog.Error(fmt.Sprintf("😢 Can't create the producer (Kafka error code %d)\n\tError: %v\n", ec, err))
				}

			} else {
				// It's not a kafka.Error
				grpcLog.Error(fmt.Sprintf("😢 Oh noes, there's a generic error creating the Producer! %v", err.Error()))
			}
			// call it when you know it's broken
			os.Exit(1)

		}

		if vGeneral.Debuglevel > 0 {
			grpcLog.Info("* Created Kafka Producer instance :")
			grpcLog.Info("")
		}
	}

	// We will use this to remember when we last flushed the kafka queues.
	vFlush := 0

	if vGeneral.MongoAtlasEnabled == 1 {

		vMongodb = loadMongoProps(arg)

		serverAPI := options.ServerAPI(options.ServerAPIVersion1)

		opts := options.Client().ApplyURI(vMongodb.Uri).SetServerAPIOptions(serverAPI)

		grpcLog.Infoln("* MongoDB URI Constructed: ", vMongodb.Uri)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		grpcLog.Infoln("* MongoDB Context Object Created")

		defer cancel()

		Mongoclient, err := mongo.Connect(ctx, opts)
		if err != nil {
			grpcLog.Fatal("Mongo Connect Failed: ", err)
		}
		grpcLog.Infoln("* MongoDB Client Connected")

		defer func() {
			if err = Mongoclient.Disconnect(ctx); err != nil {
				grpcLog.Fatal("Mongo Disconected: ", err)
			}
		}()

		// Ping the primary
		if err := Mongoclient.Ping(ctx, readpref.Primary()); err != nil {
			grpcLog.Fatal("There was a error creating the Client object, Ping failed: ", err)
		}
		grpcLog.Infoln("* MongoDB Client Pinged")

		// Create go routine to defer the closure
		defer func() {
			if err = Mongoclient.Disconnect(context.TODO()); err != nil {
				grpcLog.Fatal("Mongo Disconected: ", err)
			}
		}()

		// Define the Mongo Datastore
		appLabDatabase := Mongoclient.Database(vMongodb.Datastore)
		// Define the Mongo Collection Object
		basketcol = appLabDatabase.Collection(vMongodb.Basketcollection)
		paymentcol = appLabDatabase.Collection(vMongodb.Paymentcollection)

		if vGeneral.Debuglevel > 0 {
			grpcLog.Infoln("* MongoDB Datastore and Collections Intialized")
			grpcLog.Infoln("*")
		}

	}

	//We've said we want to safe records to file so lets initiale the file handles etc.
	if vGeneral.Json_to_file == 1 {

		// each time we run, and say we want to store the data created to disk, we create a pair of files for that run.
		// this runId is used as the file name, prepended to either _basket.json or _pmnt.json
		runId = uuid.New().String()

		// Open file -> Baskets
		loc_basket := fmt.Sprintf("%s%s%s_%s.json", vGeneral.Output_path, pathSep, runId, "basket")
		if vGeneral.Debuglevel > 2 {
			grpcLog.Infoln("Basket File          :", loc_basket)

		}

		f_basket, err = os.OpenFile(loc_basket, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			grpcLog.Errorln("os.OpenFile error A", err)
			panic(err)

		}
		defer f_basket.Close()

		// Open file -> Payment
		loc_pmnt := fmt.Sprintf("%s%s%s_%s.json", vGeneral.Output_path, pathSep, runId, "pmnt")
		if vGeneral.Debuglevel > 2 {
			grpcLog.Infoln("Payment File         :", loc_basket)

		}

		f_pmnt, err = os.OpenFile(loc_pmnt, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			grpcLog.Errorln("os.OpenFile error A", err)
			panic(err)

		}
		defer f_pmnt.Close()
	}

	// if set to 0 then we want it to simply just run and run and run. so lets give it a pretty big number
	if vGeneral.Testsize == 0 {
		vGeneral.Testsize = 10000000000000
	}

	if vGeneral.Debuglevel > 0 {
		grpcLog.Info("**** LETS GO Processing ****")
		grpcLog.Infoln("")

	}

	basketdocs := make([]interface{}, vMongodb.Batch_size)
	paymentdocs := make([]interface{}, vMongodb.Batch_size)
	msg_mongo_count := 0

	// this is to keep record of the total batch run time
	vStart := time.Now()
	for count := 0; count < vGeneral.Testsize; count++ {

		reccount := fmt.Sprintf("%v", count+1)

		if vGeneral.Debuglevel > 0 {
			grpcLog.Infoln("")
			grpcLog.Infoln("Record                        :", reccount)

		}

		// We're going to time every record and push that to prometheus
		txnStart := time.Now()

		// Build an sales basket
		pb_Basket, eventTimestamp, storeName, err := constructFakeBasket()
		if err != nil {
			os.Exit(1)

		}

		// Build an payment record for created sales basket
		pb_Payment := constructPayments(pb_Basket.InvoiceNumber, eventTimestamp, pb_Basket.Total)
		if err != nil {
			os.Exit(1)

		}

		json_SalesBasket, err := json.Marshal(pb_Basket)
		if err != nil {
			grpcLog.Errorln(fmt.Sprintf("json.Marshal %s %s ", "pb_Basket", err))
			os.Exit(1)

		}

		json_Payment, err := json.Marshal(pb_Payment)
		if err != nil {
			grpcLog.Errorln(fmt.Sprintf("json.Marshal %s %s", "pb_Payment", err))
			os.Exit(1)

		}

		// echo to screen
		if vGeneral.Debuglevel >= 2 {
			prettyJSON(string(json_SalesBasket))
			prettyJSON(string(json_Payment))
		}

		// Post to Confluent Kafka - if enabled
		if vGeneral.KafkaEnabled == 1 {

			if vGeneral.Debuglevel >= 2 {
				grpcLog.Info("")
				grpcLog.Info("Post to Confluent Kafka topics")
			}

			// Sales Basket
			offset, err := producer.ProduceMessage(&pb_Basket, vKafka.BasketTopicname, storeName)
			if err != nil {
				grpcLog.Errorln(fmt.Sprintf("producer.ProduceMessage %s %s", vKafka.BasketTopicname, err))
				os.Exit(1)
			}
			fmt.Println("pb_Basket ", offset)

			// Sales Payment
			if vGeneral.Sleep > 0 {
				n := rand.Intn(vGeneral.Sleep)
				time.Sleep(time.Duration(n) * time.Millisecond)
			}

			offset, err = producer.ProduceMessage(&pb_Payment, vKafka.PaymentTopicname, storeName)
			if err != nil {
				grpcLog.Errorln(fmt.Sprintf("producer.ProduceMessage %s %s", vKafka.PaymentTopicname, err))
				os.Exit(1)
			}
			fmt.Println("pb_Payment ", offset)

			vFlush++

			// Fush every flush_interval loops
			if vFlush == vKafka.Flush_interval {
				t := 10000
				if r := producer.Flush(t); r > 0 {
					grpcLog.Error(fmt.Sprintf("Failed to flush all messages after %d milliseconds. %d message(s) remain", t, r))

				} else {
					if vGeneral.Debuglevel >= 1 {
						grpcLog.Info(fmt.Sprintf("%s/%s, Messages flushed from the queue", count, vFlush))

					}
					vFlush = 0
				}
			}

			// Needs to move to our internal/kafka/producer.go

			//
			// We will decide if we want to keep this bit!!! or simplify it.
			//
			// Convenient way to Handle any events (back chatter) that we get
			/* go func() {
				doTerm := false
				for !doTerm {
					// The `select` blocks until one of the `case` conditions
					// are met - therefore we run it in a Go Routine.
					select {
					case ev := <-producer.Events():
						// Look at the type of Event we've received
						switch ev.(type) {

						case *kafka.Message:
							// It's a delivery report
							km := ev.(*kafka.Message)
							if km.TopicPartition.Error != nil {
								grpcLog.Error(fmt.Sprintf("☠️ Failed to send message to topic '%v'\tErr: %v",
									string(*km.TopicPartition.Topic),
									km.TopicPartition.Error))

							} else {
								if vGeneral.Debuglevel > 2 {
									grpcLog.Info(fmt.Sprintf("✅ Message delivered to topic '%v'(partition %d at offset %d)",
										string(*km.TopicPartition.Topic),
										km.TopicPartition.Partition,
										km.TopicPartition.Offset))

								}
							}

						case kafka.Error:
							// It's an error
							em := ev.(kafka.Error)
							grpcLog.Error(fmt.Sprint("☠️ Uh oh, caught an error:\n\t%v", em))

						}
					case <-termChan:
						doTerm = true

					}
				}
				close(doneChan)
			}() */

		}

		// Do we want to insertrecords/documents directly into Mongo Atlas?
		if vGeneral.MongoAtlasEnabled == 1 {
			msg_mongo_count += 1

			// Flush/insert
			// Cast a byte string to BSon
			// https://stackoverflow.com/questions/39785289/how-to-marshal-json-string-to-bson-document-for-writing-to-mongodb
			// this way we don't need to care what the source structure is, it is all cast and inserted into the defined collection.

			// Sales Basket Doc
			basketBytes, err := json.Marshal(pb_Basket)
			if err != nil {
				grpcLog.Error(fmt.Sprintf("Marchalling error: %s", err))

			}

			basketdoc, err := JsonToBson(basketBytes)
			if err != nil {
				grpcLog.Errorln("Oops, we had a problem JsonToBson converting the payload, ", err)

			}

			// Payment Doc
			paymentBytes, err := json.Marshal(pb_Payment)
			if err != nil {
				grpcLog.Error(fmt.Sprintf("Marchalling error: %s", err))

			}

			paymentdoc, err := JsonToBson(paymentBytes)
			if err != nil {
				grpcLog.Errorln("Oops, we had a problem JsonToBson converting the payload, ", err)

			}

			// Single Record inserts
			if vMongodb.Batch_size == 1 {

				// Sales Basket

				// Time to get this into the MondoDB Collection
				result, err := basketcol.InsertOne(context.TODO(), basketdoc)
				if err != nil {
					grpcLog.Errorln("Oops, we had a problem inserting (I1) the document, ", err)

				}

				if vGeneral.Debuglevel >= 2 {
					// When you run this file, it should print:
					// Document inserted with ID: ObjectID("...")
					grpcLog.Infoln("Mongo Sales Basket Doc inserted with ID: ", result.InsertedID, "\n")

				}
				if vGeneral.Debuglevel >= 3 {
					// prettyJSON takes a string which is actually JSON and makes it's pretty, and prints it.
					prettyJSON(string(json_SalesBasket))

				}

				// Payment

				// Time to get this into the MondoDB Collection
				result, err = paymentcol.InsertOne(context.TODO(), paymentdoc)
				if err != nil {
					grpcLog.Errorln("Oops, we had a problem inserting (I1) the document, ", err)

				}

				if vGeneral.Debuglevel >= 2 {
					// When you run this file, it should print:
					// Document inserted with ID: ObjectID("...")
					grpcLog.Infoln("Mongo Payment Doc inserted with ID: ", result.InsertedID, "\n")

				}
				if vGeneral.Debuglevel >= 3 {
					// prettyJSON takes a string which is actually JSON and makes it's pretty, and prints it.
					prettyJSON(string(json_Payment))

				}

			} else {

				basketdocs[msg_mongo_count-1] = basketdoc
				paymentdocs[msg_mongo_count-1] = paymentdoc

				if msg_mongo_count%vMongodb.Batch_size == 0 {

					// Time to get this into the MondoDB Collection

					// Sales Basket
					_, err = basketcol.InsertMany(context.TODO(), basketdocs)
					if err != nil {
						grpcLog.Errorln("Oops, we had a problem inserting (IM) the document, ", err)

					}
					if vGeneral.Debuglevel >= 2 {
						grpcLog.Infoln("Mongo Sale Basket Docs inserted: ", msg_mongo_count)

					}

					// Sales Payment
					_, err = paymentcol.InsertMany(context.TODO(), paymentdocs)
					if err != nil {
						grpcLog.Errorln("Oops, we had a problem inserting (IM) the document, ", err)

					}
					if vGeneral.Debuglevel >= 2 {
						grpcLog.Infoln("Mongo Payment Docs inserted: ", msg_mongo_count)

					}

					msg_mongo_count = 0

				}

			}

		}

		// Save multiple Basket docs and Payment docs to a single basket file and single payment file for the run
		if vGeneral.Json_to_file == 1 {

			if vGeneral.Debuglevel >= 2 {
				grpcLog.Info("")
				grpcLog.Info("JSON to File Flow")

			}

			// Sales Basket
			pretty_basket, err := json.MarshalIndent(pb_Basket, "", " ")
			if err != nil {
				grpcLog.Errorln("MarshalIndent error", err)

			}

			if _, err = f_basket.WriteString(string(pretty_basket) + ",\n"); err != nil {
				grpcLog.Errorln("os.WriteString error ", err)

			}

			// Sales Payment
			pretty_pmnt, err := json.MarshalIndent(pb_Payment, "", " ")
			if err != nil {
				grpcLog.Errorln("MarshalIndent error", err)

			}

			if _, err = f_pmnt.WriteString(string(pretty_pmnt) + ",\n"); err != nil {
				grpcLog.Errorln("os.WriteString error ", err)

			}

		}

		if vGeneral.Debuglevel > 1 {
			grpcLog.Infoln("Total Time                    :", time.Since(txnStart).Seconds(), "Sec")

		}

		// used to slow the data production/posting to kafka and safe to file system down.
		if vGeneral.Sleep > 0 {
			n := rand.Intn(vGeneral.Sleep) // if vGeneral.sleep = 1000, then n will be random value of 0 -> 1000  aka 0 and 1 second
			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infof("Going to sleep for            : %d Milliseconds\n", n)

			}
			time.Sleep(time.Duration(n) * time.Millisecond)
		}

	}

	grpcLog.Infoln("")
	grpcLog.Infoln("**** DONE Processing ****")
	grpcLog.Infoln("")

	vEnd := time.Now()
	vElapse := vEnd.Sub(vStart)
	grpcLog.Infoln("Start                         : ", vStart)
	grpcLog.Infoln("End                           : ", vEnd)
	grpcLog.Infoln("Elapsed Time (Seconds)        : ", vElapse.Seconds())
	grpcLog.Infoln("Records Processed             : ", vGeneral.Testsize)
	grpcLog.Infoln(fmt.Sprintf("                              :  %.3f Txns/Second", float64(vGeneral.Testsize)/vElapse.Seconds()))

	grpcLog.Infoln("")

} // runLoader()

func main() {

	var arg string

	grpcLog.Info("****** Starting           *****")

	arg = os.Args[1]

	runLoader(arg)

	grpcLog.Info("****** Completed          *****")

}
