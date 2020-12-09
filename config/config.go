package config

import "fmt"

// DBConfig is database configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	Port     int    `toml:"port" json:"port"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"-"` // omit it for privacy
	DBName   string `toml:"db-name" json:"db-name"`
}

type Config struct {
	DBConfig
	Concurrency int
}

func (c *Config) String() string {
	return fmt.Sprintf("concurrency: %v, host: %v, port: %v, user: %v, password: %v, db-name: %v",
		c.Concurrency, c.Host, c.Port, c.User, c.Password, c.DBName)
}
