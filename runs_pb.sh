
. ./.pwdloc


########################################################################
# Golang  Examples : https://developer.confluent.io/get-started/go/

########################################################################
# MongoDB Params
# Mongo -> Kafka -> MongoDB 
# https://blog.ldtalentwork.com/2020/05/26/how-to-sync-your-mongodb-databases-using-kafka-and-mongodb-kafka-connector/
#
# *_mongo.json & -> See .pws

go run -v cmd/main.go pb


# https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html
# kafkacat -b localhost:9092 -t SNDBX_AppLab