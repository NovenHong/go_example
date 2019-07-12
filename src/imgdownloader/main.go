package main

import (
    "fmt"
    "net/http"
    "io/ioutil"
    "regexp"
    "strings"
    "time"
    "strconv"
    _ "sync"
)

func main () {
  
    //urls := GetPrettyImgUrls("https://www.cj655.com/")

    //fmt.Printf("%v \n",urls)

    //var wg sync.WaitGroup
    var resChan chan *resBody
    var count int

    urls := GetPrettyImgUrls("https://www.cj655.com/")

    resChan = make(chan *resBody, len(urls))

    for _,url := range urls {
        //fmt.Printf("url:%s \n",url)
        filename := GetFilenameFromUrl(url)
        filename = "../file/" + filename
        //fmt.Printf("filename:%s \n",filename)
        //wg.Add(1)
        go DownloadImg(url,filename,resChan)
    }

    for rb := range resChan {
        count ++
        fmt.Printf("body:%+v %d\n",rb,count)
        if(count >= len(urls)){
            close(resChan)
        }
    }

    //wg.Wait()

}

func GetFilenameFromUrl(url string) (filename string) {
    index := strings.LastIndex(url,"/")
    filename = url[index+1:]
    filename = strconv.Itoa(int(time.Now().Unix())) + "_" + filename
    return filename
}

func DownloadImg(url string,filename string,resChan chan *resBody) (ok bool) {
    resp,err := http.Get(url)
    if err != nil {
        fmt.Printf("http get error: %s",err)
        return false
    }

    defer resp.Body.Close()

    data,_ := ioutil.ReadAll(resp.Body)

    err = ioutil.WriteFile(filename,data,0666)
    if err != nil {
        re := &resBody{filename,false}
        resChan <- re
        return false
    }else{
        re := &resBody{filename,true}
        resChan <- re
        return true
    }
}

func GetPrettyImgUrls(url string) (urls []string) {

    resp,err := http.Get(url)
    if err != nil {
        fmt.Printf("http get error: %s",err)
        return
    }

    defer resp.Body.Close()

    data,_ := ioutil.ReadAll(resp.Body)

    //fmt.Printf(string(data))

    re := regexp.MustCompile(`\<img\s+src=\"([^\"]*)\"\/\>`)

    results := re.FindAllStringSubmatch(string(data),-1)
    fmt.Printf("find %d results \n",len(results))

    for _,result := range results {
        url := result[1]
        if !strings.Contains(url,"http"){
            url = fmt.Sprintf("https://www.cj655.com%s",url)
        }
        //fmt.Printf("url:%s \n",url)
        urls = append(urls,url)
    }

    return urls

}