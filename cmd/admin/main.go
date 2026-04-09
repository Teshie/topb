package main

import (
	"log"
	"net/http"
	"os"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
	"github.com/qor/admin"
	"golang/internal/models"
)

func main() {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		log.Fatal("PG_DSN is empty")
	}

	// Use jinzhu/gorm (v1) for QOR Admin
	db, err := gorm.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}
	defer db.Close()

	// AutoMigrate your models so admin UI has tables
	// NOTE: list the structs you want to manage
	db.AutoMigrate(
		&models.User{},
		&models.Order{},
		&models.Product{},
		// ... add more
	)

	adm := admin.New(&admin.AdminConfig{DB: db})

	// Register resources (these become menu items in the left sidebar)
	adm.AddResource(&models.User{})
	adm.AddResource(&models.Order{})
	adm.AddResource(&models.Product{})

	// Mount admin to /admin on a basic http mux
	mux := http.NewServeMux()
	adm.MountTo("/admin", mux)

	port := os.Getenv("ADMIN_PORT")
	if port == "" {
		port = "8088"
	}
	log.Printf("QOR Admin listening on :%s (visit /admin)", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
