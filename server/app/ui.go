package app

import (
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type Page struct {
	Title string
	App *Impl
	Body []byte
}

func loadPage(title string) (*Page, error) {
	body, err := ioutil.ReadFile(indexHTML)
	if err != nil {
		return nil, err
	}
	return &Page{Title: title, App: appImpl, Body: body}, nil
}

// Forward rpc requests to the rpc server and only handle http requests here.
func indexHandler(w http.ResponseWriter, r *http.Request) {
	title := r.URL.Path[:]
	log.Printf("Handling request URL: %v", title)
	if strings.Contains(strings.ToLower(title), "rpc") {
		rpcServer.ServeHTTP(w, r)
	} else {
		/*p, err := loadPage(title)
		if err != nil {
			log.Printf("Error loading page: %v", err)
			p = &Page{Title: title, App: appImpl, Body: make([]byte, 0)}
		}*/
		p := &Page{Title: title, App: appImpl, Body: make([]byte, 0)}
		t, err := template.ParseFiles(indexHTML)
		if err != nil {
			log.Fatalf("Error parsing template file; err: %v", err)
		}
		t.Execute(w, p)
	}
}
