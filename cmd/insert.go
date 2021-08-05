package cmd

import (
	"log"
	"path/filepath"
	"strings"
	"sync"

	"github.com/TimothyStiles/poly"
	"github.com/TimothyStiles/poly/parsers/uniprot"
	"github.com/allyourbasepair/allbase/models"
	"github.com/allyourbasepair/allbase/rhea"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
)

var insertCmd = &cobra.Command{
	Use:   "insert",
	Short: "insert data from paths",
	Run: func(cmd *cobra.Command, args []string) {

		insert(args)
	},
}

func insert(args []string) {

	//get the matches from the args.
	matches := getMatches(args)
	if len(matches) == 0 {
		panic("no matches found")
	}

	// get database connection
	db, err := sqlx.Open("sqlite3", "allbase.db")
	if err != nil {
		panic(err)
	}

	// insert data
	for _, match := range matches {
		if strings.Contains(match, ".seq") {
			sequences := poly.ReadGbkFlat(match)
			err := models.GenbankInsert(db, sequences)
			if err != nil {
				log.Fatal(err)
			}
		} else if strings.Contains(match, "rhea*.rdf") {
			rhea, err := rhea.Read(match)
			if err != nil {
				log.Fatal(err)
			}

			err = models.RheaInsert(db, rhea)
			if err != nil {
				log.Fatal(err)
			}
		} else if strings.Contains(match, "rhea*.tsv") {
			err = models.RheaTsvInsert(db, match, true)
			if err != nil {
				log.Fatal(err)
			}
		} else if strings.Contains(match, "xml.gz") {
			// Insert Uniprot
			log.Printf("Inserting uniprot sprot")
			var wg sync.WaitGroup
			uniprotSprot, errors, err := uniprot.Read("match")
			if err != nil {
				log.Fatalf("Failed to read uniprot on error: %s", err)
			}
			wg.Add(1)
			go models.UniprotInsert(db, "sprot", uniprotSprot, errors, &wg)
			wg.Wait()

			for err := range errors {
				if err.Error() != "EOF" {
					log.Fatalf("Failed on error during uniprot parsing or insertion: %s", err)
				}
			}
		}

	}
}

// helper function to get unique glob patterns cli args
func getMatches(args []string) []string {
	var matches []string

	//take all args and get their pattern matches.
	for argIndex := 0; argIndex < len(args); argIndex++ {
		match, _ := filepath.Glob(args[argIndex])
		matches = append(matches, match...)
	}

	//filtering pattern matches for duplicates.
	matches = uniqueNonEmptyElementsOf(matches)

	return matches

}

// a simple helper function to remove duplicates from a list of strings.
// Used to reduce reduncy in filepath pattern matching.
// from https://gist.github.com/johnwesonga/6301924
func uniqueNonEmptyElementsOf(s []string) []string {
	unique := make(map[string]bool, len(s))
	us := make([]string, len(unique))
	for _, elem := range s {
		if len(elem) != 0 {
			if !unique[elem] {
				us = append(us, elem)
				unique[elem] = true
			}
		}
	}

	return us

}
