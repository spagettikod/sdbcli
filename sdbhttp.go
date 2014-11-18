package main

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/spagettikod/sdb"
	"html/template"
	"log"
	"net/http"
	"os"
)

var (
	tMaster, tError, tDomainList, tItems *template.Template
	errLog                               *log.Logger
)

const (
	masterHtml = `
	<!DOCTYPE html>
	<html>
	<head>
		<title>{{.Title}} - sdbcli</title>
	</head>
	<body>
		{{template "content" .}}
	</body>
	</html>`

	errorHtml = `
	{{define "content"}}
	<h2 style="color: red;">{{.}}</h2>
	{{end}}
	`

	domainListHtml = `
	{{define "content"}}
		<h2>{{.Title}}</h2>
		{{if .Data}}
			{{range .Data}}
			<a href="domain/{{.}}">{{.}}</a><br>
			{{end}}
		{{else}}
		<strong>No domains available</strong>
		{{end}}
	{{end}}
	`

	itemsHtml = `
	{{define "content"}}
		<h2>{{.Title}}</h2>
		{{if .Data}}
			<table>
			<tr>
			{{range .Data.Columns}}
				<th>{{.Name}}</th>
			{{end}}
			</tr>
			{{range .Data.Items}}
				<tr>
				<td>{{.Name}}</td>
				{{range .Attributes}}
					<td>{{.Value}}</td>
				{{end}}
				</tr>
			{{end}}
		{{else}}
		<strong>No items in domain</strong>
		{{end}}
	{{end}}
	`
)

type Page struct {
	Title string
	Data  interface{}
}

type Domain struct {
	Name string
}

func init() {
	errLog = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)
	tMaster = template.Must(template.New("master").Parse(masterHtml))

	tError = template.Must(tMaster.Clone())
	tError = template.Must(tError.Parse(errorHtml))

	tDomainList = template.Must(tMaster.Clone())
	tDomainList = template.Must(tDomainList.Parse(domainListHtml))

	tItems = template.Must(tMaster.Clone())
	tItems = template.Must(tItems.Parse(itemsHtml))
}

func redirectHandler(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/domain", http.StatusTemporaryRedirect)
}

func listDomainHandler(w http.ResponseWriter, r *http.Request) {
	resp, err := db.ListDomains()
	if err != nil {
		renderError(w, err)
		return
	}

	err = tDomainList.Execute(w, &Page{Title: "Available domains", Data: resp.DomainNames})
	if err != nil {
		renderError(w, err)
		return
	}
}

func showDomainHandler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	if name == "" {
		renderError(w, errors.New("Domain name can not be empty"))
		return
	}

	resp, err := db.Select("select * from " + name + " where ItemName() > '0' order by ItemName() desc")
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}

	type ItemCols struct {
		Columns []Column
		Items   []sdb.Item
	}

	ic := &ItemCols{}
	ic.Columns = attrlens(resp.Items)
	ic.Items = resp.Items

	err = tItems.Execute(w, &Page{Title: "Items for domain: " + name, Data: ic})
	if err != nil {
		renderError(w, err)
		return
	}
}

func renderError(w http.ResponseWriter, err error) {
	err = tError.Execute(w, &Page{Title: "Error occures", Data: err.Error()})
	if err != nil {
		errLog.Println(err)
		return
	}
}

func listen(port string) {
	fmt.Println("Listening on port " + port + "...")
	r := mux.NewRouter()
	r.HandleFunc("/", redirectHandler)
	r.HandleFunc("/domain", listDomainHandler)
	r.HandleFunc("/domain/{name}", showDomainHandler)
	http.Handle("/", r)
	http.ListenAndServe(":"+port, nil)
}
