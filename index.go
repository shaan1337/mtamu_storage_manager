package mta

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/search/query"
)

//Index struct
type Index struct {
	q          chan<- *Message
	file       string
	bleveIndex bleve.Index
}

//Init initialize index service
func (index *Index) Init() {
	_, err := os.Stat(index.file)
	if err == nil {
		index.bleveIndex, err = bleve.Open(index.file)
		if err != nil {
			fmt.Printf("[index] error opening index file: %s\n", err.Error())
		}
	} else if os.IsNotExist(err) {
		mapping := bleve.NewIndexMapping()

		fileInfoMapping := bleve.NewDocumentMapping()
		mapping.AddDocumentMapping("file_info", fileInfoMapping)

		fileInfoMapping.AddFieldMappingsAt("Path", bleve.NewTextFieldMapping())
		fileInfoMapping.AddFieldMappingsAt("Size", bleve.NewNumericFieldMapping())
		fileInfoMapping.AddFieldMappingsAt("Dir", bleve.NewBooleanFieldMapping())
		fileInfoMapping.AddFieldMappingsAt("ModTime", bleve.NewDateTimeFieldMapping())
		fileInfoMapping.AddFieldMappingsAt("BackupState", bleve.NewNumericFieldMapping())

		index.bleveIndex, err = bleve.New(index.file, mapping)
		if err != nil {
			fmt.Printf("[index] error creating new index file: %s\n", err.Error())
		}
	} else if err != nil {
		fmt.Printf("[index] error initializing index: %s\n", err.Error())
		return
	}
}

//ListAll list all index entries
func (index *Index) ListFiles(msg *Message) {
	query := bleve.NewMatchAllQuery()
	size := 10
	search := &bleve.SearchRequest{
		Query: query,
		Size:  size,
		From:  (msg.params["page"].(int) - 1) * size,
	}
	searchResults, err := index.bleveIndex.Search(search)
	if err != nil {
		params := make(map[string]interface{})
		params["error"] = true
		params["errorMsg"] = err.Error()
		res := &Message{
			msg:    "reply",
			params: params,
		}
		msg.out <- res
		return
	}

	var result SearchResult
	for _, val := range searchResults.Hits {
		doc, _ := index.bleveIndex.Document(val.ID)
		fileInfo := index.getFileInfo(doc)
		result.Files = append(result.Files, fileInfo)
	}

	params := make(map[string]interface{})
	params["result"] = result
	res := &Message{
		msg:    "reply",
		params: params,
	}
	msg.out <- res
}

func (index *Index) getFileInfo(doc *document.Document) *FileInfo {
	fileInfo := &FileInfo{}
	for _, field := range doc.Fields {
		switch field := field.(type) {
		case *document.NumericField:
			switch field.Name() {
			case "Size":
				size, _ := field.Number()
				fileInfo.Size = int64(size)
			case "BackupState":
				state, _ := field.Number()
				fileInfo.BackupState = int(state)
			}
		case *document.DateTimeField:
			switch field.Name() {
			case "ModTime":
				fileInfo.ModTime, _ = field.DateTime()
			}
		case *document.TextField:
			switch field.Name() {
			case "Path":
				path := string(field.Value())
				fileInfo.Path = string(path)
			}

		case *document.BooleanField:
			switch field.Name() {
			case "Dir":
				fileInfo.Dir, _ = field.Boolean()
			}
		}
	}
	return fileInfo
}

//SearchFiles search all index entries matching specified pattern
func (index *Index) SearchFiles(msg *Message) {
	queryString := msg.params["query"].(string)
	tokens := strings.Split(queryString, " ")
	queries := make([]query.Query, len(tokens))
	for i, token := range tokens {
		queries[i] = bleve.NewPrefixQuery(strings.ToLower(strings.Trim(token, " ")))
	}

	combinedQuery := bleve.NewDisjunctionQuery(queries...)

	size := 10
	search := &bleve.SearchRequest{
		Query: combinedQuery,
		Size:  size,
		From:  (msg.params["page"].(int) - 1) * size,
	}
	searchResults, err := index.bleveIndex.Search(search)
	if err != nil {
		params := make(map[string]interface{})
		params["error"] = true
		params["errorMsg"] = err.Error()
		res := &Message{
			msg:    "reply",
			params: params,
		}
		msg.out <- res
		return
	}

	var result SearchResult
	for _, val := range searchResults.Hits {
		doc, _ := index.bleveIndex.Document(val.ID)
		fileInfo := index.getFileInfo(doc)
		result.Files = append(result.Files, fileInfo)
	}

	params := make(map[string]interface{})
	params["result"] = result
	res := &Message{
		msg:    "reply",
		params: params,
	}
	msg.out <- res
}

//SearchResult struct
type SearchResult struct {
	Files []*FileInfo `json:"files"`
}

//FileInfo struct
type FileInfo struct {
	Path        string    `json:"path"`
	Size        int64     `json:"size"`
	Dir         bool      `json:"dir"`
	ModTime     time.Time `json:"modtime"`
	BackupState int       `json:"backupstate"`
}

func (fileInfo *FileInfo) Type() string {
	return "file_info"
}

const (
	NoBackup     int = iota
	StaleBackup  int = iota
	LatestBackup int = iota
)

func (index *Index) UpdateFileInfo(msg *Message) {
	out := msg.params["out"].(chan int)
	key := msg.params["path"].(string)
	doc, err := index.bleveIndex.Document(key)

	if err != nil {
		fmt.Printf("[index] error loading document %s: %s\n", key, err.Error())
		out <- 1
		return
	}

	fileInfo := &FileInfo{
		Path:    msg.params["path"].(string),
		Size:    msg.params["size"].(int64),
		Dir:     msg.params["dir"].(bool),
		ModTime: msg.params["modtime"].(time.Time),
	}

	if doc == nil {
		fileInfo.BackupState = NoBackup
		err := index.bleveIndex.Index(key, fileInfo)
		if err != nil {
			fmt.Printf("[index] error indexing document %s: %s\n", key, err.Error())
		}
		fmt.Printf("[index] new path: %s [%d bytes,%s]\n", key, fileInfo.Size, fileInfo.ModTime.UTC().Format(time.RFC3339))
	} else {
		fileInfo := index.getFileInfo(doc)
		docSize := fileInfo.Size
		docModTime := fileInfo.ModTime
		if docSize != fileInfo.Size || docModTime.Unix() != fileInfo.ModTime.Unix() {
			fileInfo.BackupState = StaleBackup
			err := index.bleveIndex.Index(key, fileInfo)
			if err != nil {
				fmt.Printf("[index] error updating document %s: %s\n", key, err.Error())
			}
			fmt.Printf("[index] updating path: %s [cur: %d bytes,%s] [prev: %d bytes,%s]\n", key, fileInfo.Size, fileInfo.ModTime.UTC().Format(time.RFC3339), docSize, docModTime.UTC().Format(time.RFC3339))
		}
	}

	out <- 1
}
