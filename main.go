//check duplication of short link
//check duplication of original url

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"os/exec"
)

type urlRequest struct {
	shortUrl string
	longUrl  string
}

type urlChecker struct {
	randomString string
	result       bool
}

var mysqlCreate = make(chan urlRequest, 1000)

var checkDuplication = make(chan urlChecker, 1000)
var checkDuplicationResult = make(chan urlChecker, 1000)

func main() {
	http.HandleFunc("/", handleRequest)

	//create workers for add url items to database
	for w := 1; w <= 3; w++ {
		go databaseStoringWoker(mysqlCreate)
	}

	//create workers for check random generated string duplication
	for w := 1; w <= 3; w++ {
		go checkRandomStringDuplicationInTableWorker(checkDuplication)
	}

	// start go default http server
	http.ListenAndServe(":8081", nil)
}

func handleRequest(w http.ResponseWriter, r *http.Request) {

	// for name, values := range r.Header {
	// 	// Loop over all values for the name.
	// 	for _, value := range values {
	// 		fmt.Println(name, value)
	// 	}
	// }
	reqMethod := r.Method

	switch reqMethod {
	case "POST":
		// writeRequest <- urlRequest{w,r}
		createUrlShortener(w, r)
	case "GET":
		//    readRequest <- urlRequest{w,r}
		redirectToUrlShortener(w, r)
	default:
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "Not found\n")
	}
}

/** ############################# create shorten url functions ############################# **/
func createUrlShortener(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		// in case of any error
		return
	}

	originalUrl := r.FormValue("url")
	shortenUrl := createRandomString(5)

	mysqlCreate <- urlRequest{shortenUrl, originalUrl}

	fmt.Fprintf(w, shortenUrl)
}

func createRandomString(length int) string {
	const letterBytes = "123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	randomString := ""
	for {
		for i := range b {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		}

		randomString = string(b)
		checkDuplication <- urlChecker{randomString, true}

		for item := range checkDuplicationResult {
			if item.randomString == randomString && item.result == false {
				return randomString
			} else {
				break
			}
		}

		randomString = ""
	}
}

func checkRandomStringDuplicationInTableWorker(items <-chan urlChecker) {
	for item := range items {
		db, dbError := sql.Open("mysql", "root:123456@/go")
		if dbError != nil {
			panic(dbError)
		}
		defer db.Close()

		id := 0
		sqlStatement := "SELECT id FROM url_shortener WHERE shorten_url= '" + item.randomString + "'"
		row := db.QueryRow(sqlStatement)
		err := row.Scan(&id)

		if err != nil || id == 0 {
			checkDuplicationResult <- urlChecker{item.randomString, false}
		} else {
			checkDuplicationResult <- urlChecker{item.randomString, true}
		}
	}
}

func databaseStoringWoker(items <-chan urlRequest) {
	for item := range items {
		storeInDatabase(item.shortUrl, item.longUrl)
	}
}

func storeInDatabase(shortenUrl string, originalUrl string) bool {
	db, dbError := sql.Open("mysql", "root:123456@/go")
	if dbError != nil {
		panic(dbError)
	}
	defer db.Close()

	stmt, err := db.Prepare("INSERT INTO url_shortener(original_url , shorten_url) VALUES(?,?)")
	if err != nil {
		log.Fatal(err)
	}

	res, err := stmt.Exec(originalUrl, shortenUrl)
	if err != nil {
		log.Fatal(err)
	}

	_, err2 := res.LastInsertId()
	if err2 != nil {
		log.Fatal(err)
	}

	return true
}

/** ########################### read shorten url and redirection functions #################### **/

func redirectToUrlShortener(w http.ResponseWriter, r *http.Request) bool {
	shortenUrl := strings.Split(r.URL.Path, "/")[1]
	originalUrl := ""
	if shortenUrl == "" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "reuired item not sent\n")
	} else {

		resRedis := getKey(shortenUrl)

		if resRedis != "" {
			originalUrl = resRedis
		} else {
			db, dbError := sql.Open("mysql", "root:123456@/go")
			if dbError != nil {
				panic(dbError)
			}
			defer db.Close()

			var id int
			var shorten_url string
			var original_url string
			sqlStatement := "SELECT * FROM url_shortener WHERE shorten_url= '" + shortenUrl + "'"
			row := db.QueryRow(sqlStatement)
			err := row.Scan(&id, &original_url, &shorten_url)

			if err != nil {
				// fmt.Println(id,original_url, shorten_url, err)
				w.Header().Set("Content-Type", "text/plain")
				// w.WriteHeader(http.StatusNotFound)
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, "UNKNOW ERROE !\n")
				// panic(err)
				return true
			}

			//set in redis
			setKey(shorten_url, original_url)

			originalUrl = original_url

		}

		http.Redirect(w, r, originalUrl, http.StatusSeeOther)

	}

	return true
}

/** ########################### use redis #################### **/

func setKey(key string, value string) string {
	client := exec.Command("redis-cli")
	clinetIn, _ := client.StdinPipe()
	clientOut, _ := client.StdoutPipe()
	client.Start()
	clinetIn.Write([]byte("SET " + key + " " + value))
	clinetIn.Close()
	clientBytes, _ := ioutil.ReadAll(clientOut)

	// grepCmd.Wait()
	// fmt.Println("> grep hello")
	// fmt.Println(string(clientBytes))
	return string(clientBytes)
}

func getKey(key string) string {

	client := exec.Command("redis-cli")
	clinetIn, _ := client.StdinPipe()
	clientOut, _ := client.StdoutPipe()
	client.Start()
	clinetIn.Write([]byte("GET " + key))
	clinetIn.Close()
	clientBytes, _ := ioutil.ReadAll(clientOut)

	result := string(clientBytes)
	if len(result) > 1 {
		return result
	} else {
		return ""
	}

	// return string(clientBytes)

	// lsCmd := exec.Command("bash", "-c", "ls -a -l -h")
	// lsOut, err := lsCmd.Output()
	// if err != nil {
	//     panic(err)
	// }
	// fmt.Println("> ls -a -l -h")
	// fmt.Println(string(lsOut))
}
