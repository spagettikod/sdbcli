package main

import (
	"bufio"
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/spagettikod/sdb"
	"os"
	"strings"
)

var db *sdb.SimpleDB

func exit() {
	os.Exit(0)
}

func printHelp() {
	fmt.Fprintln(os.Stdout, "")
	fmt.Fprintln(os.Stdout, "COMMANDS")
	fmt.Fprintln(os.Stdout, "  ls\t\tList all domains")
	fmt.Fprintln(os.Stdout, "  c <name>\tCreate domain with name <name>")
	fmt.Fprintln(os.Stdout, "  del <name>\tDelete domain with name <name>")
	fmt.Fprintln(os.Stdout, "  select...\tCommand starting with select will be sent as a select query to SimpleDB")
	fmt.Fprintln(os.Stdout, "  x\t\tExists the program")
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
		if len(db.Error.Errors) > 0 {
			for _, e := range db.Error.Errors {
				fmt.Fprintf(os.Stdout, "%v: %v\n", e.Code, e.Message)
			}
		} else {
			fmt.Fprintln(os.Stdout, err)
		}
		return
	}
	fmt.Fprintln(os.Stdout, "domain created")
}

func deleteDomain(name string) {
	_, err := db.DeleteDomain(name)
	if err != nil {
		if len(db.Error.Errors) > 0 {
			for _, e := range db.Error.Errors {
				fmt.Fprintf(os.Stdout, "%v: %v\n", e.Code, e.Message)
			}
		} else {
			fmt.Fprintln(os.Stdout, err)
		}
		return
	}
	fmt.Fprintln(os.Stdout, "domain deleted")
}

func query(s string) {
	r, err := db.Select(s)
	if err != nil {
		if len(db.Error.Errors) > 0 {
			for _, e := range db.Error.Errors {
				fmt.Fprintf(os.Stdout, "%v: %v\n", e.Code, e.Message)
			}
			fmt.Fprintf(os.Stdout, "%v\n", db.RawRequest)
			fmt.Fprintf(os.Stdout, "%v\n", db.RawResponse)
		} else {
			fmt.Fprintln(os.Stdout, err)
		}
		return
	}

	for _, i := range r.Items {
		fmt.Fprintf(os.Stdout, "%v:", i.Name)
		for _, a := range i.Attributes {
			fmt.Fprintf(os.Stdout, " %v=%v", a.Name, a.Value)
		}
		fmt.Fprintln(os.Stdout, "")
	}
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
	db = sdb.NewSimpleDB(c.String("accessKey"), c.String("secretKey"), sdb.SDB_REGION_EU_WEST_1)
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		s := scanner.Text()
		if s == "x" {
			exit()
		}
		if s == "ls" {
			listDomains()
		}
		if s == "" {
			printHelp()
		}
		if strings.Index(s, "c ") == 0 {
			sp := strings.Split(s, " ")
			if len(sp) == 2 {
				createDomain(sp[1])
			} else {
				fmt.Fprintln(os.Stdout, "syntax error, no name found")
			}
		}
		if strings.Index(s, "del ") == 0 {
			sp := strings.Split(s, " ")
			if len(sp) == 2 {
				deleteDomain(sp[1])
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
