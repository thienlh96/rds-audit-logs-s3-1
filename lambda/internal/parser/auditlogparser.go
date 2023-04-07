package parser

import (
	"bufio"
	"bytes"
	"io"
	"rdsauditlogss3/internal/entity"
	"strconv"
	"strings"
	"regexp"
	"time"
)

type AuditLogParser struct {
}

func NewAuditLogParser() *AuditLogParser {
	return &AuditLogParser{}
}

func (p *AuditLogParser) ParseEntries(data io.Reader, logFileTimestamp int64) ([]*entity.LogEntry, error) {
	var entries []*entity.LogEntry
	var currentEntry *entity.LogEntry

	scanner := bufio.NewScanner(data)
	for scanner.Scan() {
		txt := scanner.Text()
		if txt == "" {
			continue
		}
		re := regexp.MustCompile(`\r?\t`)
		txt=re.ReplaceAllString(txt, " ")
		record := strings.Split(txt, ",")
		if len(record)==1 {
			record = strings.Split(txt, " ")
		}
			
		ts, err := time.Parse("2006-01-02T15:04:05.000000Z", record[0])
		if err != nil {
			ts, err = time.Parse("2006-01-02T15:04:05+07:00", record[0])
		}
		
		if err != nil {
			timestamp := record[0]
			if len(timestamp) >13 {
				timestamp = timestamp[:len(timestamp)-6]
			}
			intTime, errInt := strconv.ParseInt(timestamp, 10, 64)
			if errInt == nil {
				err=nil
			}
			ts= time.Unix(intTime, 0)
		}

		if err != nil  && currentEntry == nil {
			continue
		}

		 var newTS  entity.LogEntryTimestamp
		if err == nil {
			newTS = entity.LogEntryTimestamp{
				Year:  ts.Year(),
				Month: int(ts.Month()),
				Day:   ts.Day(),
				Hour:  ts.Hour(),
			}
		}else{
			newTS = currentEntry.Timestamp
		}

		

		if currentEntry != nil && currentEntry.Timestamp != newTS {
			entries = append(entries, currentEntry)
			currentEntry = nil
		}

		if currentEntry == nil {
			currentEntry = &entity.LogEntry{
				Timestamp:        newTS,
				LogLine:          new(bytes.Buffer),
				LogFileTimestamp: logFileTimestamp,
			}
		}

		currentEntry.LogLine.WriteString(txt)
		currentEntry.LogLine.WriteString("\n")
	}

	entries = append(entries, currentEntry)

	return entries, nil
}
