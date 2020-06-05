package logger

import (
	"github.com/sirupsen/logrus"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	"log"
	"time"
)

var reqLogger *logrus.Logger

type logFormatter struct {}

func (f *logFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	line := entry.Buffer
	line.WriteString(entry.Time.UTC().Format(time.RFC3339))
	line.WriteByte(' ')
	line.WriteString(entry.Message)
	line.WriteByte('\n')
	return line.Bytes(), nil
}

func NewLogger(filePath string) *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	fileRotateLogWriter := &lumberjack.Logger{
		Filename: filePath,
		MaxSize: 10,
		MaxBackups: 5,
	}
	fileRotateLogWriter.Rotate()
	logger.SetFormatter(&logFormatter{})
	logger.SetOutput(fileRotateLogWriter)
	return logger
}

func GetLogger() *logrus.Logger {
	return reqLogger
}

func Init(filePath string) {
	reqLogger = NewLogger(filePath)
	log.Printf("Initialized logfile %s", filePath)
}