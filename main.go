package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
)

var (
	connString    = flag.String("uri", "", "connection string of atlas db eg (mongodb+srv://user:password@cluster0-mvf9w.mongodb.net/test)")
	dbString      = flag.String("db", "", "the db to use.")
	srcCollection = flag.String("src", "", "the source collection name")
	dstCollection = flag.String("dst", "", "the destination collection name")
	workers       = flag.Int("workers", 50, "number of concurrent inserts")
	progress      = flag.Bool("progress", true, "show insert progress bar")
	mongoServer   string
	dst           string
	src           string
	db            string
	ok            bool
	wg            sync.WaitGroup
	barTemplate   = `{{string . "message"}}{{counters . }} {{bar . }} {{percent . }} {{speed . "%s inserts/sec" }}`
	bar           *pb.ProgressBar
	Results       Docs
)

type Docs []bson.M

func (d Docs) clean() []bson.M {
	for i, data := range d {
		for k := range data {
			if k == "PracticeArea" || k == "FirmName" {
				switch d[i][k].(type) {
				case string:
					var a []string
					for _, v := range strings.Split(d[i][k].(string), ";") {
						a = append(a, strings.TrimSpace(v))
					}
					d[i][k] = a
				}
			}
		}
	}
	return d
}

func dbInit() {

	mongoServer, ok = os.LookupEnv("MONGODB_URI")
	if !ok {
		if *connString != "" {
			mongoServer = *connString
		} else {
			flag.Usage()
			fmt.Println()
			log.Fatal("mongodb connection string is required")
		}
	}
	db, ok = os.LookupEnv("MONGODB_DB")
	if !ok {
		if *dbString != "" {
			db = *dbString
		} else {
			flag.Usage()
			fmt.Println()
			log.Fatal("mongodb db  is required")
		}
	}
	src, ok = os.LookupEnv("MONGODB_SRC")
	if !ok {
		if *srcCollection != "" {
			src = *srcCollection
		} else {
			flag.Usage()
			fmt.Println()
			log.Fatal("source field is required")
		}
	}
	dst, ok = os.LookupEnv("MONGODB_DST")
	if !ok {
		if *dstCollection != "" {
			dst = *dstCollection
		} else {
			flag.Usage()
			fmt.Println()
			log.Fatal("source field is required")
		}
	}
}

func main() {
	flag.Parse()
	dbInit()
	log.Println("starting ")

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoServer))
	if err != nil {
		log.Fatal(err)
	}

	srcC := client.Database(db).Collection(src)
	dstC := client.Database(db).Collection(dst)

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Minute)
	cur, err := srcC.Find(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		Results = append(Results, result)
	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	if len(Results) < *workers {
		*workers = len(Results)
	}

	log.Printf("found %v records\n", len(Results))

	if *progress {
		bar = pb.ProgressBarTemplate(barTemplate).Start(len(Results)).SetMaxWidth(80)
		bar.Set("message", "Inserting docs\t")
	}
	counter := 0
	for _, v := range Results.clean() {
		wg.Add(1)
		counter++
		go func(doc bson.M) {
			defer func() {
				wg.Done()
				if *progress {
					bar.Add(1)
				}
			}()
			dstC.InsertOne(context.Background(), doc)
		}(v)
		if counter > *workers {
			wg.Wait()
			counter = 0
		}
	}
	wg.Wait()
	bar.Finish()

	log.Printf("inserted %v changed records", bar.Total())

}
