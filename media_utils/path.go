package media_utils

import (
	"github.com/wfxiang08/cyutils/utils"
	"log"
	"os"
	"path"
)

// 通过相关路径获取项目的资源时，在testcase和运行binary时的表现不太一样，各自的pwd有点点差别
func GetConfPath(filePath string) string {
	pwd, _ := os.Getwd()
	i := 0
	for !utils.FileExist(path.Join(pwd, filePath)) {
		pwd = path.Dir(pwd)
		i++
		// 找不到配置文件，就报错
		if i >= 3 {
			log.Panicf("ConfigPath not found: %s", filePath)
		}
	}
	return path.Join(pwd, filePath)

}
