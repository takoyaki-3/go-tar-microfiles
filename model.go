package gotarmicrofiles

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"math"
	"path/filepath"
)

type Storage struct {
	Dir string
	Digit int
	Core int
}

func (s *Storage)GetRawFromFile(fileKey string, raw *[]byte)error{

	key := FileName2IntegratedFileName(fileKey)[:s.Digit]

  file, err := os.Open(s.Dir+"/"+key+".tar.gz")
  defer file.Close()
	if err != nil {
		return err
	}

  // gzipの展開
  gzipReader, err := gzip.NewReader(file)
  defer gzipReader.Close()
	if err != nil {
		return err
	}

  // tarの展開
  tarReader := tar.NewReader(gzipReader)

  for tarReader != nil {
    tarHeader, err := tarReader.Next()
    if err == io.EOF {
      break
    }

    // ファイルの特定
    t := tarHeader.Name
		fmt.Println(t)
    if t == fileKey {
			*raw,err = ioutil.ReadAll(tarReader)
			return err
    }
  }
	return errors.New("file key is not found.")
}

func (s *Storage)DumpToTarFiles(orginDir string) {

	os.MkdirAll(s.Dir,077)

	wg:=sync.WaitGroup{}
	wg.Add(s.Core)

	for rank:=0;rank<s.Core;rank++{
		go func(rank int){
			defer wg.Done()

			for i:=rank;i<int(math.Pow(16,float64(s.Digit)));i+=s.Core{
			
				key := ("0000000000000000"+fmt.Sprintf("%x", i))
				key = key[len(key)-s.Digit:]

				dist, err := os.Create(s.Dir+"/"+key+".tar.gz")
				if err != nil {
					panic(err)
				}
				defer dist.Close()

				gw := gzip.NewWriter(dist)
				defer gw.Close()

				tw := tar.NewWriter(gw)
				defer tw.Close()

				// 再帰的にファイルを取得する
				if err := filepath.Walk(orginDir, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if FileName2IntegratedFileName(path)[:s.Digit] != key {
						return nil
					}

					// ディレクトリは無視
					if info.IsDir() {
						return nil
					}

					// ヘッダを書き込み
					if err := tw.WriteHeader(&tar.Header{
						Name:    path,
						Mode:    int64(info.Mode()),
						ModTime: info.ModTime(),
						Size:    info.Size(),
					}); err != nil {
						return err
					}

					// ファイルを書き込み
					f, err := os.Open(path)
					if err != nil {
						return err
					}
					defer f.Close()
					if _, err := io.Copy(tw, f); err != nil {
						return err
					}

					return nil
				}); err != nil {
					panic(err)
				}
			}
		}(rank)
	}
	wg.Wait()
}

func getBinaryBySHA256(s string) string {
	r := sha256.Sum256([]byte(s))
	return hex.EncodeToString(r[:])
}

func FileName2IntegratedFileName(s string)string{
	return getBinaryBySHA256(s)
}
