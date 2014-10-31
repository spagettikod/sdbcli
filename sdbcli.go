package main

import (
	"bufio"
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/spagettikod/sdb"
	"os"
	"strings"
	"time"
)

var (
	db sdb.SimpleDB
)

func exit() {
	os.Exit(0)
}

func printHelp() {
	fmt.Fprintln(os.Stdout, "")
	fmt.Fprintln(os.Stdout, "COMMANDS")
	fmt.Fprintln(os.Stdout, "  ls                      List all domains")
	fmt.Fprintln(os.Stdout, "  create <domain>         Create domain with name <domain>")
	fmt.Fprintln(os.Stdout, "  drop <domain>           Drop domain with name <domain>")
	fmt.Fprintln(os.Stdout, "  meta <domain>           Get metadata for domain with name <domain>")
	fmt.Fprintln(os.Stdout, "  delete <domain> <item>  Delete item with name <item> from domain named <domain>")
	fmt.Fprintln(os.Stdout, "  select...               Command starting with \"select\" will be sent as a select query to SimpleDB")
	fmt.Fprintln(os.Stdout, "  q                       Exists SimpleDB CLI")
	fmt.Fprintln(os.Stdout, "")
}

func listDomains() {
	r, err := db.ListDomains()
	if err != nil {
		fmt.Fprintln(os.Stdout, err)
		return
	}
	if len(r.DomainNames) == 0 {
		fmt.Fprintln(os.Stdout, "no domains found")
		return
	}
	for _, d := range r.DomainNames {
		fmt.Fprintln(os.Stdout, d)
	}
	return
}

func createDomain(name string) {
	_, err := db.CreateDomain(name)
	if err != nil {
		fmt.Fprintln(os.Stdout, err)
		return
	}
	fmt.Fprintln(os.Stdout, "domain created")
}

func metaDomain(name string) {
	var r sdb.DomainMetadataResponse
	r, err := db.DomainMetadata(name)
	if err != nil {
		fmt.Fprintln(os.Stdout, err)
		return
	}
	fmt.Fprintf(os.Stdout, "Metadata for domain '%v' at %v\n", name, time.Unix(r.Timestamp, 0))
	fmt.Fprintf(os.Stdout, "   ItemCount = %v\n", r.ItemCount)
	fmt.Fprintf(os.Stdout, "   ItemNamesSizeBytes = %v\n", r.ItemNamesSizeBytes)
	fmt.Fprintf(os.Stdout, "   AttributeNameCount = %v\n", r.AttributeNameCount)
	fmt.Fprintf(os.Stdout, "   AttributeValueCount = %v\n", r.AttributeValueCount)
	fmt.Fprintf(os.Stdout, "   AttributeNamesSizeBytes = %v\n", r.AttributeNamesSizeBytes)
	fmt.Fprintf(os.Stdout, "   AttributeValuesSizeBytes = %v\n", r.AttributeValuesSizeBytes)
	fmt.Fprintf(os.Stdout, "   AttributeValuesSizeBytes = %v\n", r.AttributeValuesSizeBytes)
}

func dropDomain(name string) {
	_, err := db.DeleteDomain(name)
	if err != nil {
		fmt.Fprintln(os.Stdout, err)
	}
	fmt.Fprintln(os.Stdout, "domain deleted")
}

func deleteItem(domain string, item string) {
	_, err := db.DeleteItem(domain, item)
	if err != nil {
		fmt.Fprintln(os.Stdout, err)
	}
	fmt.Fprintln(os.Stdout, "item deleted")
}

type Column struct {
	Name   string
	MaxLen int
}

func attrlens(items []sdb.Item) (cols []Column) {
	var idColName = "ItemName"
	var j int = len(idColName)
	for _, item := range items {
		if len(item.Name) > j {
			j = len(item.Name)
		}
	}
	cols = append(cols, Column{Name: idColName, MaxLen: j})
	for _, item := range items {
		for i, attr := range item.Attributes {
			offset := i + 1
			if offset >= len(cols) {
				cols = append(cols, Column{Name: attr.Name, MaxLen: 0})
			}
			if len(attr.Name) > cols[offset].MaxLen {
				cols[offset].MaxLen = len(attr.Name)
			}
			if len(attr.Value) > cols[offset].MaxLen {
				cols[offset].MaxLen = len(attr.Value)
			}
		}
	}
	return
}

func pad(s string, finalLength int, padstr string) string {
	diff := finalLength - len(s)
	if diff > 0 {
		for j := 0; j < diff; j++ {
			s = s + padstr
		}
	}
	return s
}

func query(s string) {
	r, err := db.Select(s)
	if err != nil {
		fmt.Fprintln(os.Stdout, err)
		return
	}

	cols := attrlens(r.Items)
	var cstr string
	for _, c := range cols {
		cstr = cstr + " | " + pad(c.Name, c.MaxLen, " ")
	}
	cstr = cstr + " |"
	cstr = strings.TrimSpace(cstr)
	border := pad("", len(cstr), "-")
	fmt.Fprintf(os.Stdout, "%v\n", border)
	fmt.Fprintf(os.Stdout, "%v\n", cstr)
	fmt.Fprintf(os.Stdout, "%v\n", border)
	for _, i := range r.Items {
		fmt.Fprintf(os.Stdout, "| %v |", pad(i.Name, cols[0].MaxLen, " "))
		for k, a := range i.Attributes {
			fmt.Fprintf(os.Stdout, " %v |", pad(a.Value, cols[k+1].MaxLen, " "))
		}
		fmt.Fprintln(os.Stdout, "")
	}
	fmt.Fprintf(os.Stdout, "%v\n", border)
}

func sdbcli(c *cli.Context) {
	if c.String("accessKey") == "" {
		os.Stdout.WriteString("accessKey: AWS Access Key ID is not set\n")
		return
	}
	if c.String("secretKey") == "" {
		os.Stdout.WriteString("secret: AWS Secret Key ID is not set\n")
		return
	}

	db = sdb.NewSimpleDB(c.String("accessKey"), c.String("secretKey"), sdb.SDBRegionEUWest1)

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		s := scanner.Text()
		s = strings.TrimSpace(s)
		if s == "q" {
			exit()
		}
		if s == "ls" {
			listDomains()
		}
		if s == "" {
			printHelp()
		}
		if strings.Index(s, "create ") == 0 {
			sp := strings.Split(s, " ")
			if len(sp) == 2 {
				createDomain(sp[1])
			} else {
				fmt.Fprintln(os.Stdout, "syntax error, no name found")
			}
		}
		if strings.Index(s, "meta ") == 0 {
			sp := strings.Split(s, " ")
			if len(sp) == 2 {
				metaDomain(sp[1])
			} else {
				fmt.Fprintln(os.Stdout, "syntax error, no name found")
			}
		}
		if strings.Index(s, "drop ") == 0 {
			sp := strings.Split(s, " ")
			if len(sp) == 2 {
				dropDomain(sp[1])
			} else {
				fmt.Fprintln(os.Stdout, "syntax error, no name found")
			}
		}
		if strings.Index(s, "delete ") == 0 {
			sp := strings.Split(s, " ")
			if len(sp) == 3 {
				deleteItem(sp[1], sp[2])
			} else {
				fmt.Fprintln(os.Stdout, "syntax error, no name found")
			}
		}
		if strings.Index(s, "select ") == 0 {
			sp := strings.Split(s, " ")
			if len(sp) > 2 {
				query(s)
			} else {
				fmt.Fprintln(os.Stdout, "syntax error, invalid expression")
			}
		}
		fmt.Print("> ")
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "SimpleDB CLI"
	app.Usage = "Command line interface for Amazon Web Service SimpleDB service."
	app.Action = sdbcli
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "accessKey, a",
			Usage:  "AWS Access Key ID",
			EnvVar: "AWS_ACCESS_KEY_ID",
		},
		cli.StringFlag{
			Name:   "secretKey, s",
			Usage:  "AWS Secret Key ID",
			EnvVar: "AWS_SECRET_ACCESS_KEY",
		},
	}
	app.RunAndExitOnError()
}
