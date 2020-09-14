package function

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/firestore/v1"
	"google.golang.org/api/option"
)

var projectID string
var dataset string
var table string

func init() {
	projectID = os.Getenv("PROJECT_ID")
	dataset = os.Getenv("BQ_DATASET")
	table = os.Getenv("BQ_TABLE")
}

func ExportFirestoreToGCS(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	svc, err := firestore.NewService(ctx, option.WithScopes(firestore.DatastoreScope, firestore.CloudPlatformScope))
	if err != nil {
		log.Printf("error: %s", err)
		w.WriteHeader(500)
		return
	}

	req := &firestore.GoogleFirestoreAdminV1ExportDocumentsRequest{
		OutputUriPrefix: gsPrefix(projectID),
	}

	_, err = firestore.NewProjectsDatabasesService(svc).ExportDocuments(
		savePath(projectID),
		req,
	).Context(ctx).Do()
	if err != nil {
		log.Printf("error: %s", err)
		w.WriteHeader(500)
		return
	}

	log.Println("backup successfully")
}

func LoadOnBigQuery(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	c, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Printf("error: %s", err)
		w.WriteHeader(500)
		return
	}
	prefix := gsPrefix(projectID)
	savePath := savePath(projectID)
	path := filepath.Join(prefix, savePath)
	gcsRef := bigquery.NewGCSReference(path)
	gcsRef.AllowJaggedRows = true
	myDataset := c.Dataset(dataset)
	loader := myDataset.Table(table).LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateNever

	job, err := loader.Run(ctx)
	if err != nil {
		log.Printf("error: %s", err)
		w.WriteHeader(500)
		return
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Printf("error: %s", err)
		w.WriteHeader(500)
		return
	}

	if err := status.Err(); err != nil {
		log.Printf("error: %s", err)
		w.WriteHeader(500)
		return
	}
}

func gsPrefix(projectID string) string {
	return fmt.Sprintf("gs://%s-backup-firestore", projectID)
}

func savePath(projectID string) string {
	return fmt.Sprintf("projects/%s/databases/(default)", projectID)
}
