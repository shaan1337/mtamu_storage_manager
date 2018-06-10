package mta

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/document"
)

//Index struct
type Index struct {
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

		fileInfoMapping.AddFieldMappingsAt("name", bleve.NewTextFieldMapping())
		fileInfoMapping.AddFieldMappingsAt("path", bleve.NewTextFieldMapping())
		parentPathField := bleve.NewTextFieldMapping()
		parentPathField.Analyzer = keyword.Name
		fileInfoMapping.AddFieldMappingsAt("parentpath", parentPathField)
		fileInfoMapping.AddFieldMappingsAt("size", bleve.NewNumericFieldMapping())
		fileInfoMapping.AddFieldMappingsAt("dir", bleve.NewBooleanFieldMapping())
		fileInfoMapping.AddFieldMappingsAt("modtime", bleve.NewDateTimeFieldMapping())
		fileInfoMapping.AddFieldMappingsAt("modestring", bleve.NewTextFieldMapping())
		fileInfoMapping.AddFieldMappingsAt("backupstate", bleve.NewNumericFieldMapping())

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
func (index *Index) ListFiles(page int) ([]*FileInfo, bool, error) {
	query := bleve.NewMatchAllQuery()
	batchSize := 1000
	search := &bleve.SearchRequest{
		Query: query,
		Size:  batchSize,
		From:  (page - 1) * batchSize,
	}
	searchResults, err := index.bleveIndex.Search(search)
	if err != nil {
		return nil, false, err
	}

	var fileInfos []*FileInfo
	for _, val := range searchResults.Hits {
		doc, _ := index.bleveIndex.Document(val.ID)
		fileInfo, _ := index.getFileInfo(doc)
		fileInfos = append(fileInfos, fileInfo)
	}

	var hasMore = false
	if searchResults.Hits.Len() == batchSize {
		hasMore = true
	}
	return fileInfos, hasMore, nil
}

func (index *Index) getFileInfo(doc *document.Document) (*FileInfo, error) {
	fileInfo := &FileInfo{}
	for _, field := range doc.Fields {
		switch field := field.(type) {
		case *document.NumericField:
			switch field.Name() {
			case "size":
				size, _ := field.Number()
				fileInfo.Size = int64(size)
			case "backupstate":
				state, _ := field.Number()
				fileInfo.BackupState = int(state)
			}
		case *document.DateTimeField:
			switch field.Name() {
			case "modtime":
				fileInfo.ModTime, _ = field.DateTime()
			}
		case *document.TextField:
			switch field.Name() {
			case "modestring":
				fileInfo.ModeString = string(field.Value())
				fileInfo.Mode = ParseMode(fileInfo.ModeString)
			case "name":
				name := string(field.Value())
				fileInfo.Name = name
			case "path":
				path := string(field.Value())
				fileInfo.Path = string(path)
			case "parentpath":
				parentpath := string(field.Value())
				fileInfo.ParentPath = string(parentpath)
			}
		case *document.BooleanField:
			switch field.Name() {
			case "dir":
				fileInfo.IsDir, _ = field.Boolean()
			}
		}
	}
	return fileInfo, nil
}

func ParseMode(modestring string) os.FileMode {
	const str = "dalTLDpSugct"
	var mode uint32

	for i := range modestring {
		c := modestring[len(modestring)-1-i]
		if i < 9 {
			if c != '-' {
				mode |= 1 << uint(i)
			}
		} else {
			p := strings.IndexByte(str, c)
			if p != -1 {
				mode |= 1 << uint(32-1-p)
			}
		}
	}

	return os.FileMode(mode)
}

func (index *Index) getDirectoryListing(dir string) (*DirListing, error) {
	query := bleve.NewTermQuery(dir)
	query.SetField("parentpath")
	search := bleve.NewSearchRequest(query)
	searchResults, err := index.bleveIndex.Search(search)
	if err != nil {
		return nil, err
	}

	var fileInfos []*FileInfo
	numDirs := 0
	numFiles := 0

	for _, val := range searchResults.Hits {
		doc, _ := index.bleveIndex.Document(val.ID)
		fileInfo, err := index.getFileInfo(doc)
		if err != nil {
			return nil, err
		}
		if fileInfo.IsDir {
			numDirs++
		} else {
			numFiles++
		}
		fileInfos = append(fileInfos, fileInfo)
	}

	listing := &DirListing{
		Items:    fileInfos,
		NumDirs:  numDirs,
		NumFiles: numFiles,
	}

	return listing, nil
}

/*
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
		fileInfo, _ := index.getFileInfo(doc)
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
*/

//FileInfo struct
type FileInfo struct {
	Name        string      `json:"name"`
	Path        string      `json:"path"`
	ParentPath  string      `json:"parentpath"`
	Size        int64       `json:"size"`
	IsDir       bool        `json:"dir"`
	ModTime     time.Time   `json:"modtime"`
	Mode        os.FileMode `json:"mode"`
	ModeString  string      `json:"modestring"`
	BackupState int         `json:"backupstate"`
}

//DirListing struct
type DirListing struct {
	Items    []*FileInfo `json:"items"`
	NumDirs  int         `json:"numDirs"`
	NumFiles int         `json:"numFiles"`
	Sort     string      `json:"sort"`
	Order    string      `json:"order"`
}

func (fileInfo *FileInfo) Type() string {
	return "file_info"
}

const (
	NoBackup     int = iota
	StaleBackup  int = iota
	LatestBackup int = iota
)

func (idx *Index) GetFileInfo(file string) (*FileInfo, error) {
	doc, err := idx.bleveIndex.Document(file)
	if err != nil {
		return nil, fmt.Errorf("file not found: %s", file)
	}
	return idx.getFileInfo(doc)
}

func (idx *Index) GetDirectoryListing(dir string) (*DirListing, error) {
	_, err := idx.bleveIndex.Document(dir)
	if err != nil {
		return nil, fmt.Errorf("directory not found: %s", dir)
	}
	return idx.getDirectoryListing(dir)
}

func (idx *Index) UpdateFileInfo(path string, fileInfo *FileInfo, out chan bool) {
	key := path
	doc, err := idx.bleveIndex.Document(key)

	if err != nil {
		fmt.Printf("[index] error loading document %s: %s\n", key, err.Error())
		out <- false
		return
	}

	if doc == nil {
		fileInfo.BackupState = NoBackup
		err := idx.bleveIndex.Index(key, fileInfo)
		if err != nil {
			fmt.Printf("[index] error indexing document %s: %s\n", key, err.Error())
		}
		fmt.Printf("[index] new path: %s [%d bytes,%s]\n", key, fileInfo.Size, fileInfo.ModTime.UTC().Format(time.RFC3339))
	} else {
		prevFileInfo, _ := idx.getFileInfo(doc)
		if prevFileInfo != nil && (fileInfo.Size != prevFileInfo.Size || fileInfo.ModTime.Unix() != prevFileInfo.ModTime.Unix()) {
			fileInfo.BackupState = StaleBackup
			err := idx.bleveIndex.Index(key, fileInfo)
			if err != nil {
				fmt.Printf("[index] error updating document %s: %s\n", key, err.Error())
			}
			fmt.Printf("[index] updating path: %s [cur: %d bytes,%s] [prev: %d bytes,%s]\n", key, fileInfo.Size, fileInfo.ModTime.UTC().Format(time.RFC3339), prevFileInfo.Size, prevFileInfo.ModTime.UTC().Format(time.RFC3339))
		}
	}

	out <- true
}

func (idx *Index) DeleteFileInfo(path string) error {
	fmt.Printf("[index] deleting path: %s\n", path)
	return idx.bleveIndex.Delete(path)
}
