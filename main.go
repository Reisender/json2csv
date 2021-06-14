package main

import (
	"context"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"sync"

	"github.com/MasteryConnect/pipe/line"
	"github.com/MasteryConnect/pipe/message"

	"github.com/MasteryConnect/pipe/extras/csv"
	"github.com/MasteryConnect/pipe/extras/json"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
)

var (
	schemaCueFile string
	stopOnError   bool
)

func init() {
	flag.StringVar(&schemaCueFile, "schema", "", "[optional] the path to the cuelang schema file")
	flag.BoolVar(&stopOnError, "stop", false, "[optional] stop the pipeline if there is a schema validation error")
}

func main() {
	flag.Parse()
	cueCtx := cuecontext.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	toCSV := &csv.To{ShowHeader: true}

	pipe := line.New()

	if schemaCueFile != "" {
		schemaStr, err := ioutil.ReadFile(schemaCueFile)
		if err != nil {
			log.Fatal(err)
			return
		}
		schema := cueCtx.CompileBytes(schemaStr)

		if stopOnError {
			pipe.Add(stopOnCancel(ctx))
		}

		pipe.Add(newValidator(cueCtx, schema, cancel))
	}

	pipe.Add(
		line.I(json.From),
		extractHeader(toCSV),
		runToCSVLater(toCSV),
		line.Stdout,
	).RunContext(ctx)
}

func runToCSVLater(toCSV *csv.To) line.Tfunc {
	return func(in <-chan interface{}, out chan<- interface{}, errs chan<- error) {
		subIn := make(chan interface{})
		subOut := make(chan interface{})

		// used to wait for the drail go routine to copmlete
		var wg sync.WaitGroup

		spunUp := false
		for m := range in {
			if !spunUp {
				spunUp = true

				// spin up sub transformer
				go func() {
					defer close(subOut)
					toCSV.T(subIn, subOut, errs)
				}()

				// drain to out
				wg.Add(1)
				go func() {
					defer wg.Done()
					for msg := range subOut {
						out <- msg
					}
				}()
			}

			// send to toCSV.T
			subIn <- m
		}

		// close the toCSV subIn chan since the original "in" chan is closed
		close(subIn)

		// wait for drain
		wg.Wait()
	}
}

func extractHeader(toCSV *csv.To) line.Tfunc {
	return line.I(func(m interface{}) (interface{}, error) {
		if len(toCSV.Header) == 0 {
			msg := m.(map[string]interface{})
			for k, _ := range msg {
				toCSV.Header = append(toCSV.Header, k)
			}
		}
		return m, nil
	})
}

func newValidator(cueCtx *cue.Context, schema cue.Value, cancel func()) line.Tfunc {
	return line.I(func(m interface{}) (interface{}, error) {
		val := cueCtx.CompileString(message.String(m))
		err := schema.Subsume(val)
		if err != nil {
			cancel()
			return nil, err
		}
		return m, nil
	})
}

func stopOnCancel(ctx context.Context) line.Tfunc {
	return func(in <-chan interface{}, out chan<- interface{}, errs chan<- error) {
		for m := range in {
			select {
			case <-ctx.Done():
				errs <- errors.New("context cancelled")
				return
			default:
				out <- m
			}
		}
	}
}
