# MongoCreator - GoTransactionProducer

Hi all… as shared originally on the Discord channel, Veronica asked that I post here also…

<I do need to give this some more thought, will do a diagram of my thoughts, and then see when I I start this… just busy with some AWS training atm>.

If anyone want to potentially also be involved, welcome to ping me. happy to share the lime light, or is that blame… :wink:

G

… hi all…

no immediate schedule/plan for this… as I need to do some urgent AWS studying/work for employer…

but was thinking of doing a project and then documenting it and sharing the git repo, to the community.

Basic idea.
Golang app that generate fake sales (maybe split as a basket onto one kafka topic and then a payment onto another, implying the 2 was out of band), then sinking the topic/s into MongoDB using a sink connectors.
At this point I want to show a 2nd stream to it all, and do it via Python. was thinking…
maybe based on data sinked into the MongoDB store, do a trigger… with a do a source connector out of Mongo onto Kafka (some aggregating) and then consume that via the Python app and for simplistic just echo this to the console (implying it can be pushed somewhere further)

# Using the app.

This application generates fake data (sales baskets), how that is done is controlled by the *_app.json configuration file that configures the run environment. The *_seed.json in which seed data is provided is used to generate the fake basket + basket items and the associated payment document.

the *_app.json contains comments to explain the impact of the value.

on the Mac (and Linux) platform you can run the program by executing run_producer.sh
a similar bat file can be configured on Windows

The User can always start up multiple copies, specify/hard code the store, and configure one store to have small baskets, low quantity per basket and configure a second run to have larger baskets, more quantity per product, thus higher value baskets.

# Note: Not included in the repo is a file called .pwd

Example: 
export Sasl_password=Vj8MASendaIs0j4r34rsdfe4Vc8LG6cZ1XWilAJjYS05bZIk7AaGx0Y49xb 
export Sasl_username=3MZ4dfgsdfdfIUUA

This files is executed via the runs_producer.sh file, reading the values into local environment, from where they are injested by a os.Getenv call, if this code is pushed into a docker container then these values can be pushed into a secret included in the environment.

My Version numbering.
0.2	- 10/01/2024	Pushing/posting basket docs and associated payment docs onto Kafka.
0.3	- 24/01/2024	To "circumvent" Confluent Kafka cluster "unavailability" at this time I'm modifying the code here to insert directly into
					Mongo Atlas into 2 collections. This will allow the Creator community to interface with the inbound docs on the Atlas environment
					irrespective how they got there.