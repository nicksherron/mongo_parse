# mongo_parse

### Usage
download and untar file newest version from [releases](https://github.com/nicksherron/mongo_parse/releases)

```
$ wget https://github.com/nicksherron/mongo_parse/releases/download/v0.1.3/mongo_parse_v0.1.3_darwin_amd64.tar.gz\
    && tar -xvf mongo_parse_v0.1.3_darwin_amd64.tar.gz
```
```
$ cd mongo_parse_v0.1.3_darwin_amd64
```
```
$ ./mongo_parse -h
Usage of ./mongo_parse:
  -db string
    	the db to use.
  -dst string
    	the destination collection name
  -progress
    	show insert progress bar (default true)
  -src string
    	the source collection name
  -uri string
    	connection string of atlas db eg (mongodb+srv://user:password@cluster0-mvf9w.mongodb.net/test)
  -workers int
    	number of concurrent inserts (default 50)
```

####  Example 
```
$ ./mongo_parse \
    -uri "mongodb+srv://user:password@cluster0-mvf9w.mongodb.net/test" \
    -db data \
    -src source_collection \
    -dst destination_collection
```


Options can either be set from the command line (as shown above) or by the following environment variables.

| variable    | usage                                                                                          |
|-------------|------------------------------------------------------------------------------------------------|
| MONGODB_URI | connection string of atlas db eg (mongodb+srv://user:password@cluster0-mvf9w.mongodb.net/test) |
| MONGODB_DB  | the db to use                                                                                  |
| MONGODB_SRC | the source collection name                                                                     |
| MONGODB_DST | the destination collection name                                                                |


