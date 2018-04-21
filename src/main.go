package main

import (
    "fmt"
    "gopkg.in/kothar/brotli-go.v0/dec"
    "gopkg.in/kothar/brotli-go.v0/enc"
    "github.com/go-redis/redis"
    "net/http"
    "io/ioutil"
    "strings"
    "strconv"
    "golang.org/x/net/html"
    "github.com/yhat/scrape"
    "golang.org/x/net/html/atom"
)

const (
    limit = 1272
    redisHash = "ebooks"
    baseLink = "http://23.95.221.108/page/"
    baseBook = "http://23.95.221.108"
    cores = 4
)

type KeyPair struct {
    key string
    val []byte
}

func request(url string) []byte {
    resp, err := http.Get(url)
    if err != nil {
        panic(err)
    }

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        panic(err)
    }

    defer resp.Body.Close()
    return body
}

func main() {
    client := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })
    defer client.Close()

    hash, _ := client.HGetAll("ebooks").Result()
    redisChan := make(chan KeyPair, 1000)
    go func() {
        for {
            pair := <- redisChan

            comp, err := enc.CompressBuffer(nil, pair.val, make([]byte, 0))
            if err != nil {
                panic(err)
            }
            fmt.Printf("Inserting pair (%s, len %d)\n", pair.key, len(comp))
            client.HSet(redisHash, pair.key, comp)
        }
    }()

    get := func(url string) string {
        if val, ok := hash[url]; ok {
            fmt.Printf("Hit Http cache for url %s\n", url)
            decomped, _ := dec.DecompressBuffer([]byte(val), make([]byte, 0))
            return string(decomped)
        } else {
            fmt.Printf("Missed Http cache for url %s\n", url)
            htmlBody := request(url)
            redisChan <- KeyPair{url, htmlBody}

            return string(htmlBody)
        }
    }

    links := make(chan []string, limit)
    linkThread := func(j int) {
        for i := j; i <= limit; i += cores {
            url := strings.Join([]string{baseLink, strconv.Itoa(i)}, "")
            bod := get(url)
            root, err := html.Parse(strings.NewReader(bod))
            if err != nil {
                panic(err)
            }

            article := scrape.FindAll(root, scrape.ByTag(atom.Article))

            miniLinks := make([]string, 0)
            for as := range article {
                a := article[as]
                link, err := scrape.Find(a, scrape.ByTag(atom.A))
                if !err {
                    panic(err)
                }

                href := scrape.Attr(link, "href")
                trimmed := strings.TrimPrefix(href, "https://it-eb.com")
                miniLinks = append(miniLinks, trimmed)
            }

            links <- miniLinks
        }
    }
    for j := 1; j <= cores; j += 1 {
        go linkThread(j)
    }

    allLinks := make([]string, 0)
    for i := 1; i < limit; i += 1 {
        receivedLinks := <- links
        for k := range receivedLinks {
            str := receivedLinks[k]
            allLinks = append(allLinks, str)
        }
    }

    fmt.Printf("Total elements is %d\n", len(allLinks))

    type BookDetails struct {
        title string
        img string
        id string
        details map[string]string
        desc string
    }

    books := make(chan BookDetails, 1000)
    bookThread := func(i int) {
        for j := i; j < len(allLinks); j += cores {
            path := allLinks[j]
            url := strings.Join([]string{baseBook, path}, "")
            htmlBody := get(url)
            root, err := html.Parse(strings.NewReader(htmlBody))
            if err != nil {
                panic(err)
            }

            title, ok := scrape.Find(root, scrape.ByClass("post-title"))
            if !ok {
                panic(ok)
            }

            img, ok := scrape.Find(root, scrape.ByClass("book-cover"))
            if !ok {
                panic(ok)
            }

            imgText := scrape.Attr(img.FirstChild, "src")
            imgTextTrimmed := strings.TrimPrefix(imgText, "https://it-eb.com")

            desc, ok := scrape.Find(root, scrape.ByClass("entry-inner"))
            if !ok {
                panic(ok)
            }

            idMatch := func (node *html.Node) bool {
                return node.DataAtom == atom.Input && scrape.Attr(node, "type") == "hidden" && scrape.Attr(node, "name") == "comment_post_ID"
            }

            id, ok := scrape.Find(root, idMatch)
            if !ok {
                panic(ok)
            }

            idVal := scrape.Attr(id, "value")

            bookdetailsMap := make(map[string]string)
            {
                bookdetails, ok := scrape.Find(root, scrape.ByClass("book-details"))
                if !ok {
                    panic(ok)
                }

                bookdetailsKeys := scrape.FindAll(bookdetails, scrape.ByTag(atom.Span))
                bookdetailsVals := scrape.FindAll(bookdetails, scrape.ByTag(atom.Li))
                for i := range bookdetailsKeys {
                    key := bookdetailsKeys[i]
                    val := bookdetailsVals[i]

                    keyStr := scrape.Text(key)
                    valStr := scrape.Text(val)

                    trimKey := strings.ToLower(strings.TrimSuffix(keyStr, ":"))
                    trimVal := strings.TrimPrefix(valStr, keyStr)
                    bookdetailsMap[trimKey] = trimVal
                }
            }

            books <- BookDetails{scrape.Text(title), imgTextTrimmed, idVal, bookdetailsMap, scrape.Text(desc)}
        }
    }

    for i := 1; i <= cores; i += 1 {
        go bookThread(i)
    }

    allBooks := make([]BookDetails, 0)
    for i := 1; i < len(allLinks); i += 1 {
        receivedBook := <- books
        fmt.Println(receivedBook)
        allBooks = append(allBooks, receivedBook)
    }
}
