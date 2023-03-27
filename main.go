package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/briandowns/spinner"
	"github.com/google/uuid"
)

type dbClient struct {
	sc *spanner.Client
}

var dbString = os.Getenv("SPANNER_STRING")

func init() {
	fmt.Println("Start", time.Now())
}

func newClient(ctx context.Context, dbString string) (dbClient, error) {

	client, err := spanner.NewClient(ctx, dbString)
	if err != nil {
		return dbClient{}, err
	}
	return dbClient{
		sc: client,
	}, nil
}

func deleteAll(ctx context.Context, d dbClient, tableName string) error {
	defer d.sc.Close()

	m := []*spanner.Mutation{
		spanner.Delete(tableName, spanner.AllKeys()),
	}

	s := spinner.New(spinner.CharSets[9], 200*time.Millisecond)
	s.Start()
	_, err := d.sc.Apply(ctx, m)
	s.Stop()
	return err
}

func main() {
	deleteMode := flag.Bool("delete-all", false, "")
	flag.Parse()

	ctx := context.TODO()
	d, _ := newClient(ctx, dbString)

	if *deleteMode {
		deleteAll(ctx, d, "test")
		fmt.Println("All record deleted")
		return
	}

	defer func() {
		fmt.Println("End", time.Now())
	}()

	txnForEach(ctx, d)

}

func txnForEach(ctx context.Context, d dbClient) {
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		panic(err)
	}

	var n = 1
	sql := `insert into test (id, name, time) values (@id, @name, @time)`
N:
	for {
		_, err := d.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			t := time.Now().In(jst)
			id, _ := uuid.NewRandom()

			prepared := spanner.Statement{
				SQL: sql,
				Params: map[string]any{
					"name": id.String(),
					"id":   id.String(),
					"time": t.Format(time.RFC3339),
				},
			}
			count, err := txn.Update(ctx, prepared)
			if err != nil {
				log.Println(err)
				return err
			}
			if n%10000 == 0 {
				log.Println(n, id, count)
			}
			return nil
		})
		if err != nil {
			log.Println(err)
			continue N
		}
		n++
	}

}
