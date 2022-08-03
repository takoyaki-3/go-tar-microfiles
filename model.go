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
	"math"
	"os"
	"path/filepath"
	"github.com/takoyaki-3/goc"
)

type Storage struct {
	Dir string
	Digit int
	Core int
}

func (s *Storage)GetRawFromReader(r io.Reader, fileKey string, raw *[]byte)error{
	// fileKey = strings.Replace(fileKey,"\\","/",-1)

  // gzipの展開
  gzipReader, err := gzip.NewReader(r)
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
    if t == fileKey {
			*raw,err = ioutil.ReadAll(tarReader)
			return err
    }
  }
	return errors.New("file key is not found.")
}

func (s *Storage)GetRawFromFile(fileKey string, raw *[]byte)error{

	// fileKey = strings.Replace(fileKey,"\\","/",-1)
	key := FileName2IntegratedFileName(fileKey)[:s.Digit]

  file, err := os.Open(s.Dir+"/"+key+".tar.gz")
  defer file.Close()
	if err != nil {
		return err
	}

  return s.GetRawFromReader(file,fileKey,raw)
}

func (s *Storage)DumpToTarFiles(orginDir string) {

	os.MkdirAll(s.Dir,077)

	goc.Parallel(s.Core,int(math.Pow(16,float64(s.Digit))),func(i, rank int) {
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
			// path = strings.Replace(path,"\\","/",-1)
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
	})
}

func getBinaryBySHA256(s string) string {
	r := sha256.Sum256([]byte(s))
	return hex.EncodeToString(r[:])
}

func FileName2IntegratedFileName(s string)string{
	return getBinaryBySHA256(s)
}
