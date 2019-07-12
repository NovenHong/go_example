package main

import (
	"fmt"
	"os"
	"log"
	"io/ioutil"
	"strings"
)

var (
	inputPath = "C:\\\\Users\\Administrator\\AppData\\Local\\Netease\\CloudMusic\\Cache\\Cache"
	outputPath string
)

func main()  {
	
	args := os.Args[1:]
	if len(args) == 0 {
		log.Printf("Pleas type the input path and out path \n")
		return
	}
	if len(args) == 1 {
		outputPath = args[0]
	}
	if len(args) == 2 {
		inputPath = args[0]
		outputPath = args[1]
	}

	fmt.Printf("input path:%s output path:%s \n",inputPath,outputPath)
	if err := os.MkdirAll(outputPath,0777); err != nil {
		log.Fatal(err)
		return
	}
	
	files,_ := ioutil.ReadDir(inputPath)

	ch := make(chan string, 1024)
	count := 0
	length := 0

	for _,file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(),".uc") {
			length ++
			log.Printf("filename:%s",file.Name())
			go func(filename string) {
				log.Printf("parsing %s \n",filename)
				new_filename := strings.Replace(filename,".uc","",-1)
				err := decodeFile(inputPath,filename,outputPath,new_filename+".mp3")
				res := fmt.Sprintf("parse finish:%s err:%s",filename,err)
				defer func(){
					ch <- res
				}()
			}(file.Name())
		}
	}

	for result := range ch {
		count ++
		log.Printf("%s --%d\n",result,count)
		
		if count == length {
			close(ch)
		}
	}
}

func decodeFile(inputdir string,filename string,outputdir string,new_filename string) (err error) {
	bytes,err := ioutil.ReadFile(inputdir+"\\"+filename)
	if err != nil {
		return err
	}
	for i := 0; i < len(bytes); i++ {
        bytes[i] ^= 0xa3
	}
	return ioutil.WriteFile(outputdir+"\\"+new_filename,bytes,0777)
}