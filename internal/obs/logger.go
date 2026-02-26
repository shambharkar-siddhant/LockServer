package obs

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

type Logger struct {
	l *log.Logger
}

func NewLogger() *Logger {
	return &Logger{
		l: log.New(os.Stdout, "", 0),
	}
}

func (lg *Logger) Info(fields map[string]interface{}) {
	fields["level"] = "info"
	fields["ts"] = time.Now().UTC().Format(time.RFC3339Nano)

	b, _ := json.Marshal(fields)
	lg.l.Println(string(b))
}

func (lg *Logger) Error(fields map[string]interface{}) {
	fields["level"] = "error"
	fields["ts"] = time.Now().UTC().Format(time.RFC3339Nano)

	b, _ := json.Marshal(fields)
	lg.l.Println(string(b))
}