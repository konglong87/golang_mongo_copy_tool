package main

import (
	"flag"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"log"
	"os"
)

/*
author: kl--[konglong]
*/
const (
	FromAddr  = `mongodb://120.21.66.16:27017?minPoolSize=10&maxIdleTimeMS=10000` //获取数据的数据库地址
	LimitPer  = 1000                                                              //默认每次迁移的条数,
	FromDB    = "db_name1"                                                        //数据源的库名DataBaseName
	FromTable = "kl_user"                                                         //数据源的表名tableName

	//将要导入的数据库地址
	ToAddr = `mongodb://127.0.0.1:27017` // localhost有时不可以
	ToDB   = "kl_test"                   //被导入的库名
)

var (
	help      bool
	limit     int
	fromdb    string
	todb      string
	fromAddr  string
	toAddr    string
	fromTable string
	toTable   string
)

//参数校验
func checkParam() bool {
	if help {
		flag.Usage()
		return false
	}
	//3个参数不能为空
	if fromdb == "" || fromTable == "" || todb == "" {
		flag.Usage()
		return false
	}
	// 3个默认值
	if fromAddr == "" {
		fromAddr = FromAddr
	}
	if toAddr == "" {
		toAddr = ToAddr
	}
	if toTable == "" {
		toTable = fromTable
	}
	return true
}
func main() {

	if bo := checkParam(); !bo {
		return
	}

	log.Printf("fromaddr==%s, fromtable==%s, toaddr==%s,todb==%s,totable==%s\n", fromAddr, fromTable, toAddr, todb, toTable)

	log.Println("start...")

	//连接数据源
	fromClient, err := GetDBSession(fromAddr, fromdb, fromTable)
	if err != nil {
		log.Println("[error]source mongo server dial error:", err)
		return
	}
	defer fromClient.Database.Session.Close()

	num, err := fromClient.Find(nil).Count()
	if err != nil {
		log.Println("查询总数出错:", err)
		return
	}
	log.Println("total number is:", num)
	//计算分批次数
	i := num / LimitPer
	if num%LimitPer != 0 {
		i++
	}

	//连接需要导入的数据库
	ToClient, err := GetDBSession(toAddr, todb, toTable)
	if err != nil {
		log.Println("[error] import mongo server dial error.:", err)
		return
	}
	defer ToClient.Database.Session.Close()

	//
	for ii := 0; ii < i; ii++ {
		res := fromClient.Find(nil).Sort("_id").Limit(LimitPer).Skip(ii * LimitPer) //每次查询LimitPer条
		if ii*LimitPer+LimitPer > num {
			log.Println(ii*LimitPer, "<=============coping===============>", num)
		} else {
			log.Println(ii*LimitPer, "<=============coping===============>", ii*LimitPer+LimitPer)
		}
		var mapSli []map[string]interface{}
		if err := res.All(&mapSli); err != nil {
			log.Println("err===>", err)
		}

		//开始分批导入
		for k := 0; k < len(mapSli); k++ {
			_, errInsert := ToClient.UpsertId(mapSli[k]["_id"],
				bson.M{
					"$set": mapSli[k],
				},
			)
			if errInsert != nil {
				log.Println("mongo insert is error.:", errInsert)
				return
			}
		}
	}
	log.Println("[copy data end]")
}

func GetDBSession(addr, databaseName, collectionName string) (*mgo.Collection, error) {
	session, err := mgo.Dial(addr)
	log.Println("debug dial mongo addr is:", addr)
	if err != nil {
		log.Println("[error]mgo Dial error is:", err)
		return nil, err
	}
	sessionCopy := session.Copy()
	c := sessionCopy.DB(databaseName).C(collectionName)
	return c, nil
}
func init() {
	flag.BoolVar(&help, "h", false, `-h 将会展示详细参数说明：  `)
	flag.IntVar(&limit, "limit", 1000, `auhd`)
	flag.StringVar(&fromAddr, "fromaddr", "", `-fromaddr  数据源服务器地址 例如:-fromaddr=mongodb://localhost:27017`)
	flag.StringVar(&fromdb, "fromdb", "", `-fromdb  数据源的库名，例如-fromdb=cont_filter`)
	flag.StringVar(&fromTable, "fromtable", "", `-fromtable  数据源的表名，例如-fromtable=cont_tag`)
	flag.StringVar(&toAddr, "toaddr", "", `-toaddr  被导入数据库地址，例如-toaddr=mongodb://localhost:27017`)
	flag.StringVar(&todb, "todb", "", `-todb  被导入的库名，例如-todb=copy_db `)
	flag.StringVar(&toTable, "totable", "", `-totable  被导入的表名，例如-totable=cont_tag`)
	flag.Usage = use
	flag.Parse()

}
func use() {
	fmt.Fprintf(os.Stderr, `******************copy data**************************
    Usage:  
    ***[-fromaddr 原数据库地址(比如："mongodb://localhost:27017")] [-fromdb(必填) 原数据库的库名] [-fromtable(必填) 原数据库表名]  
    ***[-toaddr 被导入数据库地址] [-todb(必填) 被导入数据库库名][-totable 被导入数据库表名(不填的话默认与fromtable相同)]
    *** 比如linux上 ./copydata -fromdb=content_filter -fromtable=cont_tag  -todb=con_copy -totable=cont_tag 
    *** 比如window上  copydata.exe -fromdb=content_filter -fromtable=cont_tag  -todb=con_copy -totable=cont_tag 
    *** 参数等号前后不能有空格!!!

Arguments(参数详细说明如下):
`)
	flag.PrintDefaults()
}
