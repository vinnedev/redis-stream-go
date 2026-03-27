package config

import (
	"bufio"
	"errors"
	"os"
	"strconv"
	"strings"
)

// LoadDotEnv reads key/value pairs from .env files without overriding existing env vars.
func LoadDotEnv(filenames ...string) error {
	if len(filenames) == 0 {
		filenames = []string{".env"}
	}

	for _, filename := range filenames {
		if err := loadEnvFile(filename); err != nil {
			return err
		}
	}

	return nil
}

func loadEnvFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			continue
		}

		if len(value) >= 2 {
			switch value[0] {
			case '"', '\'':
				if unquoted, err := strconv.Unquote(value); err == nil {
					value = unquoted
				} else {
					value = strings.Trim(value, `"'`)
				}
			}
		}

		if _, exists := os.LookupEnv(key); exists {
			continue
		}

		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}

	return scanner.Err()
}
