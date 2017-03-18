package main

import (
	"runtime"
	"time"

	"database/sql"

	_ "net/http/pprof"

	"github.com/Sirupsen/logrus"
	"github.com/buger/jsonparser"
	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/contrib/ginrus"
	"github.com/gin-gonic/gin"
	"github.com/golang/snappy"
	"github.com/kelseyhightower/envconfig"
	_ "github.com/lib/pq"
	_ "github.com/mattes/migrate/driver/postgres"
	"github.com/mattes/migrate/migrate"
	nsq "github.com/nsqio/go-nsq"
)

// Config holds the application's configuration info from the environment.
type Config struct {
	LogLevel    string `default:"info" split_words:"true"`
	PostgresURL string `default:"postgres://order-server@localhost:5432/order-server?sslmode=disable" envconfig:"postgres_url"`
	NSQURL      string `default:"nsqd:4150" envconfig:"nsq_url"`
	Port        string `default:"8000" envconfig:"port"`
}

var db *sql.DB

func main() {
	config := loadConfig()
	connectToDB(config)
	migrateDB(config)
	startNSQConsumer(config)
	startWebServer(config)

	// Terminate this goroutine, crash if all other goroutines exited
	runtime.Goexit()
}

// Initialize databse connection pool
func connectToDB(config Config) {
	var err error
	db, err = sql.Open("postgres", config.PostgresURL)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(20)
}

// Check database and migrate if needed.
func migrateDB(config Config) {
	errors, ok := migrate.UpSync(config.PostgresURL, "./migrations")
	if !ok {
		logrus.Error("Migrating the database failed!")
		for _, err := range errors {
			logrus.Error(err.Error())
		}
		panic(0)
	}
}

// Start the NSQ consumer and caches.
func startNSQConsumer(config Config) {
	nsqConfig := nsq.NewConfig()
	nsqConfig.Snappy = false
	nsqConfig.MaxInFlight = 100

	consumer, err := nsq.NewConsumer("orders", "order-store", nsqConfig)
	if err != nil {
		panic(err)
	}

	consumer.AddConcurrentHandlers(nsq.HandlerFunc(handleMessage), 20)

	err = consumer.ConnectToNSQD(config.NSQURL)
	if err != nil {
		panic(err)
	}
}

// Handle messages from NSQ
func handleMessage(message *nsq.Message) error {
	regionID, err := jsonparser.GetInt(message.Body, "regionID")
	if err != nil {
		logrus.Warnf("Error parsing regionID: %s", err.Error())
		return err
	}

	typeID, err := jsonparser.GetInt(message.Body, "typeID")
	if err != nil {
		logrus.Warnf("Error parsing typeID: %s", err.Error())
		return err
	}

	var compressedRowset []byte
	compressedRowset = snappy.Encode(compressedRowset, message.Body)

	query := `INSERT INTO markets ("regionID", "typeID", "market") VALUES ($1, $2, $3) ON CONFLICT ("regionID", "typeID") DO UPDATE SET "market" = EXCLUDED."market"`
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(query, regionID, typeID, compressedRowset)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Start the webserver.
func startWebServer(config Config) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(cors.Default())
	router.Use(ginrus.Ginrus(logrus.StandardLogger(), time.RFC3339, true))

	v1 := router.Group("/api/orders/v1")
	v1.GET("/region/:regionID/", getRegion)
	v1.GET("/type/:typeID/", getType)
	v1.GET("/region/:regionID/type/:typeID/", getRegionType)

	router.Run(":" + config.Port)
}

func getRegion(context *gin.Context) {
	regionID := context.Param("regionID")

	query := `SELECT "market" FROM "markets" WHERE "regionID" = $1`
	tx, err := db.Begin()
	if err != nil {
		context.AbortWithError(500, err)
		return
	}

	rows, err := tx.Query(query, regionID)
	if err != nil {
		context.AbortWithError(500, err)
		return
	}

	response, err := concatRowsToJSON(rows)
	if err != nil {
		context.AbortWithError(404, err)
		return
	}

	err = tx.Commit()
	if err != nil {
		context.AbortWithError(500, err)
		return
	}

	context.Header("Access-Control-Allow-Origin", "*")
	context.Data(200, "application/json; charset=utf-8", response)
}

func getType(context *gin.Context) {
	typeID := context.Param("typeID")

	query := `SELECT "market" FROM "markets" WHERE "typeID" = $1`
	tx, err := db.Begin()
	if err != nil {
		context.AbortWithError(500, err)
		return
	}

	rows, err := tx.Query(query, typeID)
	if err != nil {
		context.AbortWithError(500, err)
		return
	}

	response, err := concatRowsToJSON(rows)
	if err != nil {
		context.AbortWithError(404, err)
		return
	}

	err = tx.Commit()
	if err != nil {
		context.AbortWithError(500, err)
		return
	}

	context.Header("Access-Control-Allow-Origin", "*")
	context.Data(200, "application/json; charset=utf-8", response)
}

func getRegionType(context *gin.Context) {
	regionID := context.Param("regionID")
	typeID := context.Param("typeID")

	query := `SELECT "market" FROM "markets" WHERE "regionID" = $1 AND "typeID" = $2`
	tx, err := db.Begin()
	if err != nil {
		context.AbortWithError(500, err)
		return
	}

	rows, err := tx.Query(query, regionID, typeID)
	if err != nil {
		context.AbortWithError(500, err)
		return
	}

	response, err := concatRowsToJSON(rows)
	if err != nil {
		context.AbortWithError(404, err)
		return
	}

	err = tx.Commit()
	if err != nil {
		context.AbortWithError(500, err)
		return
	}

	context.Header("Access-Control-Allow-Origin", "*")
	context.Data(200, "application/json; charset=utf-8", response)
}

func concatRowsToJSON(rows *sql.Rows) ([]byte, error) {
	defer rows.Close()

	response := []byte("[")
	for rows.Next() {
		var market []byte

		err := rows.Scan(&market)
		if err != nil {
			return nil, err
		}

		var decompressedMarket []byte
		decompressedMarket, err = snappy.Decode(decompressedMarket, market)
		if err != nil {
			return nil, err
		}

		orders, _, _, err := jsonparser.Get(decompressedMarket, "orders")
		if err != nil {
			return nil, err
		}

		response = append(response, orders[1:len(orders)-1]...)
		response = append(response, ","...)
	}

	if len(response) > 1 {
		response = response[:len(response)-1]
	}
	response = append(response, "]"...)

	return response, nil
}

// Load configuration from environment
func loadConfig() Config {
	config := Config{}
	envconfig.MustProcess("ORDER_SERVER", &config)

	logLevel, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		panic(err)
	}

	logrus.SetLevel(logLevel)
	logrus.Debugf("Config: %q", config)
	return config
}
