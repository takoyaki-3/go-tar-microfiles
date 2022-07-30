package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	gb "github.com/takoyaki-3/go-binaryfile"
	gt "github.com/takoyaki-3/go-tar-microfiles"
)

func main(){

	// テスト用ファイル作成
	fmt.Println("create test files.")
	os.MkdirAll("test",0777)
	for i:=0;i<1000;i++{
		raw := []byte("hello, this is "+strconv.Itoa(i)+" file.")
		gb.DumpToFile(&raw,"test/"+strconv.Itoa(i)+".txt")
	}

	// ファイルの統合
	fmt.Println("dump tar files.")
	s := gt.Storage{
		Dir: "./testData",
		Digit: 2,
	}
	s.DumpToTarFiles("test")

	// 読み込み
	fmt.Println("load file from key.")
	var raw []byte
	if err := s.GetRawFromFile("test\\0.txt",&raw);err!=nil{
		log.Fatalln(err)
	}
	fmt.Println(string(raw))
}

