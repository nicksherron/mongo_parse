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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
)

type Doc bson.M

func (d Doc) clean() Doc {

	switch d["PracticeArea"].(type) {
	case string:
		var a []string
		for _, v := range strings.Split(d["PracticeArea"].(string), ";") {
			a = append(a, strings.TrimSpace(v))
		}
		d["PracticeArea"] = a
	}
	switch d["FirmName"].(type) {
	case string:
		var a []string
		for _, v := range strings.Split(d["FirmName"].(string), ";") {
			a = append(a, strings.TrimSpace(v))
		}
		d["FirmName"] = a
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

func insert(doc Doc, client *mongo.Client) {

	dstC := client.Database(db).Collection(dst)

	defer func() {
		wg.Done()
		if *progress {
			bar.Add(1)
		}
	}()
	// create new record since order not preserved
	out := bson.D{
		{"_id", doc["_id"]},
		{"FName", doc["FName"]},
		{"MName", doc["MName"]},
		{"LName", doc["LName"]},
		{"Suffix", doc["Suffix"]},
		{"PracticeArea", doc["PracticeArea"]},
		{"FirmName", doc["FirmName"]},
		{"Address", doc["Address"]},
		{"City", doc["City"]},
		{"State", doc["State"]},
		{"Zip", doc["Zip"]},
		{"Email", doc["Email"]},
		{"Website", doc["Website"]},
		{"Phone", doc["Phone"]},
		{"Mobile", doc["Mobile"]},
		{"Fax", doc["Fax"]},
		{"ContactLegacyID", doc["ContactLegacyID"]},
		{"CompanyLegacyID", doc["CompanyLegacyID"]},
		{"SFContactID", doc["SFContactID"]},
		{"SFCompanyID", doc["SFCompanyID"]},
		{"Status", doc["Status"]},
		{"Bar", doc["Bar"]},
		{"BarYear", doc["BarYear"]},
		{"Rating", doc["Rating"]},
		{"RatingFactors", doc["RatingFactors"]},
		{"FirmRating", doc["FirmRating"]},
		{"Languages", doc["Languages"]},
		{"AdvancedDegrees", doc["AdvancedDegrees"]},
		{"Sections", doc["Sections"]},
		{"StateCourts", doc["StateCourts"]},
		{"FederalCourts", doc["FederalCourts"]},
		{"BoardCerts", doc["BoardCerts"]},
		{"NationalCerts", doc["NationalCerts"]},
		{"StateAdmitted", doc["StateAdmitted"]},
		{"Companies", doc["Companies"]},
		{"Undergrad", doc["Undergrad"]},
		{"CircuitDistrict", doc["CircuitDistrict"]},
		{"LawSchoolName", doc["LawSchoolName"]},
		{"StateAdmission", doc["StateAdmission"]},
		{"Associations", doc["Associations"]},
		{"AwardName", doc["AwardName"]},
		{"SourceName", doc["SourceName"]},
		{"Updated", doc["Updated"]},
		{"Avvo_Rating", doc["Avvo_Rating"]},
		{"BIO", doc["BIO"]},
		{"AttorneyID", doc["AttorneyID"]},
		{"MartinAwardURL", doc["MartinAwardURL"]},
		{"MartinPhotoURL", doc["MartinPhotoURL"]},
		{"ClientRating", doc["ClientRating"]},
		{"ReerRating", doc["ReerRating"]},
		{"ISLN", doc["ISLN"]},
		{"RegistryImageURL", doc["RegistryImageURL"]},
		{"AdmitYear", doc["AdmitYear"]},
		{"BarStatus", doc["BarStatus"]},
		{"StateBarAdmissions", doc["StateBarAdmissions"]},
		{"BarSource", doc["BarSource"]},
		{"RegistryData", doc["RegistryData"]},
		{"NonFirmCompany", doc["NonFirmCompany"]},
		{"OriginalDataSource", doc["OriginalDataSource"]},
		{"GroupID", doc["GroupID"]},
		{"RecordID", doc["RecordID"]},
	}
	dstC.InsertOne(context.TODO(), out)
}

func main() {
	start := time.Now()
	flag.Parse()
	dbInit()
	log.Println("starting ")

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoServer))
	if err != nil {
		log.Fatal(err)
	}

	srcC := client.Database(db).Collection(src)

	count, err := srcC.CountDocuments(context.TODO(), bson.M{})

	if *progress {
		bar = pb.ProgressBarTemplate(barTemplate).Start(int(count)).SetMaxWidth(70)
		bar.Set("message", "Inserting docs\t")
	}

	cur, err := srcC.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	counter := 0
	for cur.Next(context.TODO()) {
		var result Doc
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		counter++
		go insert(result.clean(), client)
		if counter > *workers {
			wg.Wait()
			counter = 0
		}
	}
	wg.Wait()
	bar.Finish()

	log.Printf("inserted %v records in %v", bar.Total(), time.Since(start))
}
