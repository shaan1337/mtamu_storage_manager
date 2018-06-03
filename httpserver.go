package mta

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

var httpMessageQueue chan<- *Message

//HTTPServer struct
type HTTPServer struct {
	q chan<- *Message
}

//Init initialize server
func (server *HTTPServer) Init() {
	httpMessageQueue = server.q
}

//StartListening start listening for web requests
func (server *HTTPServer) StartListening() {
	http.HandleFunc("/files", filesHandler)
	http.HandleFunc("/search", searchHandler)
	http.HandleFunc("/backup", backupHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func filesHandler(w http.ResponseWriter, r *http.Request) {
	output := make(chan *Message)
	params := make(map[string]interface{})
	params["page"] = 1
	page := r.URL.Query().Get("page")
	if len(page) != 0 {
		pageInt, err := strconv.Atoi(page)
		if err == nil {
			params["page"] = pageInt
		}
	}

	var msg = &Message{
		msg:    "list_files",
		params: params,
		out:    output,
	}
	httpMessageQueue <- msg
	res := <-output
	if res.params["error"] != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "An error has occured: %s", res.params["errorMsg"])
	} else {
		searchResult := res.params["result"].(SearchResult)
		searchResultJSON, _ := json.Marshal(searchResult)
		fmt.Fprintf(w, "%s", string(searchResultJSON))
	}
	close(output)
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	output := make(chan *Message)
	params := make(map[string]interface{})

	params["query"] = r.URL.Query().Get("query")
	params["page"] = 1
	page := r.URL.Query().Get("page")
	if len(page) != 0 {
		pageInt, err := strconv.Atoi(page)
		if err == nil {
			params["page"] = pageInt
		}
	}

	var msg = &Message{
		msg:    "search_files",
		params: params,
		out:    output,
	}
	httpMessageQueue <- msg
	res := <-output
	if res.params["error"] != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "An error has occured: %s", res.params["errorMsg"])
	} else {
		searchResult := res.params["result"].(SearchResult)
		searchResultJSON, _ := json.Marshal(searchResult)
		fmt.Fprintf(w, "%s", string(searchResultJSON))
	}
	close(output)
}

func backupHandler(w http.ResponseWriter, r *http.Request) {
	output := make(chan *Message)
	var msg = &Message{
		msg: "backup_file",
		out: output,
	}
	httpMessageQueue <- msg
	res := <-output
	fmt.Fprintf(w, "%s", res.msg)
	close(output)
}
