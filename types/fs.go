package types

// Structs - the values we bring in from *app.json configuration file
type Tp_general struct {
	EchoConfig        int
	Hostname          string
	Debuglevel        int
	Testsize          int     // Used to limit number of records posted, over rided when reading test cases from input_source,
	Sleep             int     // sleep time between Basket Create and Payment post
	SeedFile          string  // Which seed file to read in
	EchoSeed          int     // 0/1 Echo the seed data to terminal
	CurrentPath       string  // current
	OSName            string  // OS name
	Vatrate           float64 // Amount
	Store             int     // if <> 0 then store at that position in array is selected.
	KafkaEnabled      int     // if = 1 then post docs to kafka
	MongoAtlasEnabled int     // if = 1 then post docs to MongoDB
	Json_to_file      int     // do we spool the created baskets and payments to a file/s
	Output_path       string  // if yes above then pipe json here. we will spool the baskets to one file and the payments to a second.
	TimeOffset        string  // what offset do we run with, from GMT / Zulu time
	Max_items_basket  int     // max items in a basket
	Max_quantity      int     // max quantity of items in a basket per product
	KafkaConfigFile   string  // Kafka configuration file
	MongoConfigFile   string  // Mongo configuration file
}

type TKafka struct {
	EchoConfig        int
	Bootstrapservers  string
	SchemaRegistryURL string
	BasketTopicname   string
	PaymentTopicname  string
	Numpartitions     int
	Replicationfactor int
	Retension         string
	Parseduration     string
	Security_protocol string
	Sasl_mechanisms   string
	Sasl_username     string
	Sasl_password     string
	Flush_interval    int
}

type TMongodb struct {
	Url               string
	Uri               string
	Root              string
	Port              string
	Username          string
	Password          string
	Datastore         string
	Basketcollection  string
	Paymentcollection string
	Batch_size        int
}

type Tp_BasketItem struct {
	Id       string  `json:"id,omitempty"`
	Name     string  `json:"name,omitempty"`
	Brand    string  `json:"brand,omitempty"`
	Category string  `json:"category,omitempty"`
	Price    float64 `json:"price,omitempty"`
	Quantity int     `json:"quantity,omitempty"`
}

/* type Tp_basket struct {
	InvoiceNumber string          `json:"invoiceNumber,omitempty"`
	SaleDateTime  string          `json:"saleDateTime,omitempty"`
	SaleTimestamp string          `json:"saleTimestamp,omitempty"`
	Store         TStoreStruct    `json:"store,omitempty"`
	Clerk         TPClerkStruct   `json:"clerk,omitempty"`
	TerminalPoint string          `json:"terminalPoint,omitempty"`
	BasketItems   []Tp_BasketItem `json:"basketItems,omitempty"`
	Nett          float64         `json:"nett,omitempty"`
	VAT           float64         `json:"vat,omitempty"`
	Total         float64         `json:"total,omitempty"`
} */

/* type Tp_payment struct {
	InvoiceNumber    string  `json:"invoiceNumber,omitempty"`
	PayDateTime      string  `json:"payDateTime,omitempty"`
	PayTimestamp     string  `json:"payTimestamp,omitempty"`
	Paid             float64 `json:"paid,omitempty"`
	FinTransactionID string  `json:"finTransactionId,omitempty"`
} */

type TPClerkStruct struct {
	Id   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type TStoreStruct struct {
	Id   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type TProductStruct struct {
	Id       string  `json:"id,omitempty"`
	Name     string  `json:"name,omitempty"`
	Brand    string  `json:"brand,omitempty"`
	Category string  `json:"category,omitempty"`
	Price    float64 `json:"price,omitempty"`
}

type TPSeed struct {
	Clerks   []TPClerkStruct  `json:"clerks,omitempty"`
	Stores   []TStoreStruct   `json:"stores,omitempty"`
	Products []TProductStruct `json:"products,omitempty"`
}
