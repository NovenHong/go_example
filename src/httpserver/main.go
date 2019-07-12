package main

import (
    "fmt"
    "net/http"
    "strings"
    "sync"
    "time"
    "math/rand"
    "crypto/md5"
    "io/ioutil"
    "log"
    "path/filepath"
    _ "encoding/hex"
    "os"
    "io"
)

var userlist []string
var mu sync.Mutex
const Upload_Dir = "../file/"

//fmt.Printf("%x",md5.Sum([]byte("test")))

func main () {
    http.HandleFunc("/",Viewhandler)
    http.HandleFunc("/Get",GetHandler)
    http.HandleFunc("/PostImport",PostImport)
    http.HandleFunc("/GetLucky",GetLucky)
    http.HandleFunc("/Upload",Upload)
    fmt.Printf("127.0.0.1:8000 listening... \n")
    http.ListenAndServe("127.0.0.1:8000",nil)
}

func GetHandler(w http.ResponseWriter, r *http.Request) {

    count := len(userlist)

    resp := fmt.Sprintf("当前总共参与抽奖的用户数：%d\n",count)

    w.Write([]byte(resp))
}

func PostImport(w http.ResponseWriter, r *http.Request) {

    r.ParseForm()

    users_param := r.Form["users"]

    users := strings.Split(users_param[0],",")

    count1 := len(userlist)

    mu.Lock()
    defer mu.Unlock()

    for _,user := range users {
        user = strings.TrimSpace(user)
        userlist = append(userlist,user)
    }

    count2 := len(userlist)

    resp := fmt.Sprintf("当前总共参与抽奖的用户数： %d，成功导入的用户数:%d \n", count2, count2-count1)

    w.Write([]byte(resp))

}

func GetLucky(w http.ResponseWriter, r *http.Request) {

    mu.Lock()
    mu.Unlock()

    count := len(userlist)

    if count >= 1 {
        seed := time.Now().UnixNano()
        rand.Seed(seed)
        index := rand.Int31n(int32(count))
        user := userlist[index]
        userlist = append(userlist[0:index],userlist[index+1:]...)

        //fmt.Printf("index:%d user:%s userlist:%v \n",index,user,userlist)

        resp := fmt.Sprintf("当前中奖用户:%s, 剩余用户数:%d \n",user,len(userlist))
        w.Write([]byte(resp))

    } else {
        resp := fmt.Sprintf("当前没有参与抽奖的用户，请通过/PostImport导入用户\n")
        w.Write([]byte(resp))

    }

}

func Viewhandler(w http.ResponseWriter, r *http.Request) {
    path := r.URL.Path[1:]

    data,err := ioutil.ReadFile("../file/"+path)
    if err != nil {
        log.Printf("path:%s error:%s \n",path,err)
        w.WriteHeader(404)
        w.Write([]byte("404 not found"))
    }

    if strings.HasSuffix(path,"html") {
        w.Header().Add("Content-Type", "text/html")
    }else if strings.HasSuffix(path,"mp4") {
        w.Header().Add("Content-Type", "video/mp4")
    }

    w.Write(data)
}

func Upload(w http.ResponseWriter, r *http.Request) {
    r.ParseMultipartForm(32 << 20) //32m

    file,handler,err := r.FormFile("file")
    if err != nil {
        fmt.Fprintf(w,"upload err:%s",err)
        return
    }

    ext := filepath.Ext(handler.Filename)

    if ext != ".mp4" {
        fmt.Fprintf(w,"no support ext:%s",ext)
        return
    }

    md5_str := md5.Sum([]byte(string(time.Now().UnixNano())))
    filename := fmt.Sprintf("%x%s",md5_str,ext)

    f,_ := os.OpenFile(Upload_Dir+filename, os.O_CREATE|os.O_WRONLY, 0660)
    _,err = io.Copy(f,file)
    if err != nil {
        fmt.Fprintf(w,"upload err:%s",err)
    }

    filedir,_ := filepath.Abs(Upload_Dir+filename)
    fmt.Fprintf(w,"success, file dir:%s \n",filedir)
}