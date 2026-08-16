package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	PrefectAPIURL string
	NameSpace     string
	Image         string
	Port          string
}

func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	return &Config{
		PrefectAPIURL: os.Getenv("PREFECT_API_URL"),
		NameSpace:     os.Getenv("NAMESPACE"),
		Image:         os.Getenv("IMAGE"),
		Port:          os.Getenv("PORT"),
	}
}
