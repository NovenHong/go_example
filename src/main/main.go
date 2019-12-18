package main

import (
	"fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
	"strings"
	"strconv"
	//"sync"
	"log"
	"os"
	"math"
	"flag"
	"path/filepath"
)

const (
    USERNAME = "root"
    //PASSWORD = ""//game123456
    NETWORK  = "tcp"
    SERVER   = "localhost"
    PORT     = 3306
    DATABASE = "cj655"
)

var PASSWORD string = ""

type UserData struct {
	UserId int64 `db:"user_id"`
	ChannelId int64 `db:"channel_id"`
}

type UserPlayData struct {
	UserId int64 `db:"user_id"`
	UniqueFlagCount int `db:"unique_flag_count"`
	Date string `db:"date"`
	DateTime int64
}

type UserPlayDataDetail struct {
	UserId int64 `db:"user_id"`
	UniqueFlag sql.NullString `db:"unique_flag"`
}

type MaxSameUniqueFlagCount struct {
	UniqueFlag string
	Count int
}

type ChannelUserLoginData struct {
	Id int64
	ChannelId int64
	UserId int64
	SameUniqueFlagCount int
	UniqueFlagCount int
	UpdateTime int64
	Date string
	DateTime int64
}

var logDirPath string
var startTimeDate string
var endTimeDate string
var startTime int64
var endTime int64
var DB *sql.DB
var err error
var failureLogger *log.Logger
var runtimeLogger *log.Logger
var loc *time.Location
var page int = 1
var date string
var quarySql string
var quarySql2 string
var count float64
var limit int
var delay int

var allStartTime int64
var allEndTime int64

var pageSuccessCount int
var pageErrorCount int
var totalCount int64 = 0
var totalSuccessCount int64 = 0
var totalErrorCount int64 = 0


func init() {

	myOS := os.Getenv("OS")
	if myOS == "Windows_NT" {
		PASSWORD = ""
	}else{
		PASSWORD = "game123456"
	}

	
	DB = openDB()

	loc, _ = time.LoadLocation("Local")

	monthDays := getFirstLastMonthDay(time.Now().Format("2006-01"))

	startTimeDate = monthDays["firstDay"]
	endTimeDate = monthDays["lastDay"]

	initFlag()

	//startTime :=  time.Now().AddDate(0, -2, 0).Format("2006-01-02")
	theTime, _ := time.ParseInLocation("2006-01-02", startTimeDate, loc)
	startTime = theTime.Unix()

	//endTimeDate := time.Now().Format("2006-01-02")
	theTime, _ = time.ParseInLocation("2006-01-02", endTimeDate, loc)
	endTime = theTime.Unix()+86399

	date = theTime.Format("2006-01")

	//fmt.Println(startTime)
	//fmt.Println(endTime)

	initLog()

}

func main() {

	if delay > 0 {
		timer := time.NewTimer(time.Second*time.Duration(delay))
		<-timer.C
		fmt.Println("delay end,start task")
	}

	listRow := 1000

	totalPage,err := getTotalPage(listRow)
	if err != nil{
		fmt.Printf("QuaryCountSql failed,Error:%v",err)
		return
	}

	allStartTime = time.Now().Unix()

	for {

		if page % 10 ==  0 {
			DB.Close()
			time.Sleep(time.Second * 5)
			DB = openDB()
		}

		pageStartTime := time.Now().Unix()
		pageSuccessCount = 0
		pageErrorCount = 0

		if(page > totalPage){

			allEndTime = time.Now().Unix()

			runtimeLogger.Output(0,fmt.Sprintf("All page is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s \n\n",
			totalSuccessCount,totalErrorCount,totalCount,resolveSecond(allEndTime-allStartTime)))
			fmt.Println(fmt.Sprintf("All page is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
			totalSuccessCount,totalErrorCount,totalCount,resolveSecond(allEndTime-allStartTime)))
			break
		}

		firstRow := (page-1)*listRow

		userDatas := getUserDatas(firstRow,listRow)

		userDatasLength := len(userDatas)
		if userDatasLength == 0 {
			pageLog(page,totalPage,pageSuccessCount,pageErrorCount,"null")
			page++
			time.Sleep(time.Second * 1)
			continue
		}

		userIds := getUserIds(userDatas)

		userPlayDatas := getUserPlayDatas(userIds)

		userPlayDatasLength := len(userPlayDatas)

		userDataDetails := getUserDataDetails(userIds)

		userDataDetailsLength := len(userDataDetails)

		if userPlayDatasLength == 0 || userDataDetailsLength == 0 {
			pageLog(page,totalPage,pageSuccessCount,pageErrorCount,"null")
			page++
			time.Sleep(time.Second * 1)
			continue
		}

		for _,userData := range userDatas {

			channelUserLoginData := getChannelUserLoginData(userData,userPlayDatas,userDataDetails)

			//fmt.Println(channelUserLoginData)
			
			saveChannelUserLoginData(channelUserLoginData)

		}

		pageEndTime := time.Now().Unix()

		pageLog(page,totalPage,pageSuccessCount,pageErrorCount,resolveSecond(pageEndTime-pageStartTime))

		page++
		time.Sleep(time.Second * 1)
		//DB.Close()

		
	}

	endLog()
}

func openDB() (DB *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s",USERNAME,PASSWORD,NETWORK,SERVER,PORT,DATABASE)
	DB,err = sql.Open("mysql",dsn)
	
	if err != nil{
        panic(fmt.Sprintf("Open mysql failed,Error:%v\n",err))
	}

	DB.SetConnMaxLifetime(100*time.Second)  //最大连接周期，超过时间的连接就close
    DB.SetMaxOpenConns(100)//设置最大连接数
	DB.SetMaxIdleConns(16) //设置闲置连接数

	return
}

func pageLog(page int,totalPage int,pageSuccessCount int,pageErrorCount int,time string) {

	runtimeLogger.Output(0,fmt.Sprintf("Page:%d/%d SuccessRow:%d ErrorRow:%d Time:%s \n\n",
		page,totalPage,pageSuccessCount,pageErrorCount,time))

	if limit != 0 {
		fmt.Println(fmt.Sprintf("Page %d/%d is compeleted,SuccessRow:%d ErrorRow:%d Time: %s",
		page,totalPage,pageSuccessCount,pageErrorCount,time))
	}		

}

func initFlag() {
	var currentPage int
	var currentMonth string
	flag.IntVar(&currentPage,"page",0,"当前Page页码")
	flag.StringVar(&currentMonth,"month","","当前的月份")
	flag.IntVar(&limit,"limit",0,"全部条数")
	flag.IntVar(&delay,"delay",0,"延迟执行秒数")
	flag.Parse()

	if currentPage != 0 {
		page = currentPage
	}
	if currentMonth != "" {

		monthDays := getFirstLastMonthDay(currentMonth)

		startTimeDate = monthDays["firstDay"]
		endTimeDate = monthDays["lastDay"]
	}
}

func getFirstLastMonthDay(monthDate string) (monthDays map[string]string) {
	theTime, _ := time.ParseInLocation("2006-01", monthDate, loc)
	year,month,_ := theTime.Date()

	firstMonthUTC :=time.Date(year,month,1,0,0,0,0,loc)
	firstMonthDay := firstMonthUTC.Format("2006-01-02")
	lastMonthDay := firstMonthUTC.AddDate(0, 1, -1).Format("2006-01-02")

	monthDays = map[string]string{}

	monthDays["firstDay"] = firstMonthDay
	monthDays["lastDay"] = lastMonthDay

	return
}

func initLog() {

	myOS := os.Getenv("OS")
	if myOS == "Windows_NT" {
		logDirPath = "./log"
	}else{
		path, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		logDirPath = fmt.Sprintf("%s/log",path)
	}

	//create log dir
	_, err := os.Stat(logDirPath)
	if os.IsNotExist(err) {
		os.Mkdir(logDirPath, os.ModePerm)
	}

	//log
	failureLogFile, _ := os.OpenFile(fmt.Sprintf("%s/failure-%s.log",logDirPath,date), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	failureLogger = log.New(failureLogFile,"",log.Ldate | log.Ltime)

	runtimeLogFile,_ := os.OpenFile(fmt.Sprintf("%s/runtime-%s.log",logDirPath,date), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	runtimeLogger = log.New(runtimeLogFile,"",log.Ldate | log.Ltime)

	beginLog()
}

func beginLog() {
	failureLogger.Output(0,fmt.Sprintf("\n\n========== Begin =========="))
	runtimeLogger.Output(0,fmt.Sprintf("\n\n========== Begin =========="))
}

func endLog() {
	failureLogger.Output(0,fmt.Sprintf("\n========== End ==========\n\n"))
	runtimeLogger.Output(0,fmt.Sprintf("\n========== End ==========\n\n"))
}

func getTotalPage(listRow int) (totalPage int,err error) {

	if limit != 0 {
		count = float64(limit)
	}else {
		row := DB.QueryRow("SELECT count(distinct user_id) as count FROM gc_user")
		err = row.Scan(&count);
	}

	totalPage = int(math.Ceil(count/float64(listRow)))

	return
}



func getUserDataDetails(userIds []string) (userDataDetails []UserPlayDataDetail) {

	if len(userIds) == 0 {
		return
	}

	quarySql = fmt.Sprintf(`SELECT unique_flag,user_id 
	FROM gc_user_play_data 
	WHERE ( user_id in ( %s ) ) AND ( ( login_time BETWEEN %d AND %d ) )`,strings.Join(userIds,","),startTime,endTime)

	rows, err := DB.Query(quarySql)

	if err != nil {
		failureLogger.Output(0,fmt.Sprintf("Page:%d Sql:%s Error:%v",page,quarySql,err))
		return
	}

	userPlayDataDetail := new(UserPlayDataDetail)

	userDataDetails = []UserPlayDataDetail{}

	for rows.Next() {
		rows.Scan(&userPlayDataDetail.UniqueFlag,&userPlayDataDetail.UserId)
		userDataDetails = append(userDataDetails,*userPlayDataDetail)
	}

	defer func() {
		rows.Close()
	}()

	return
}


func getUserDatas(firstRow int,listRow int) (userDatas []UserData) {

	if limit == 0 {
		quarySql = fmt.Sprintf(`SELECT user_id,channel_id FROM gc_user WHERE (user_id is not null) AND (channel_id is not null) ORDER BY user_id ASC LIMIT %d,%d`,firstRow,listRow)
	}else {
		quarySql = fmt.Sprintf(`SELECT user_id,channel_id FROM gc_user WHERE (user_id is not null) AND (channel_id is not null) ORDER BY user_id DESC LIMIT %d,%d`,firstRow,listRow)
	}

	rows, err := DB.Query(quarySql)

	if err != nil {
		failureLogger.Output(0,fmt.Sprintf("Page:%d Sql:%s Error:%v",page,quarySql,err))
		return
	}

	userData := new(UserData)

	for rows.Next() {
		rows.Scan(&userData.UserId,&userData.ChannelId)
		userDatas = append(userDatas,*userData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getUserIds(userDatas []UserData) (userIds []string) {

	userIds = []string{}
	for _,value := range userDatas {
		userIds = append(userIds,strconv.FormatInt(value.UserId,10))
	}

	return
}


func getUserPlayDatas(userIds []string) (userPlayDatas []UserPlayData) {

	quarySql = fmt.Sprintf(`SELECT user_id,count(unique_flag) unique_flag_count,FROM_UNIXTIME(login_time,'%%Y-%%m') as date 
	FROM gc_user_play_data WHERE ( user_id in ( %s ) ) AND ( ( login_time BETWEEN %d AND %d ) ) GROUP BY user_id`,
	strings.Join(userIds,","),startTime,endTime)

	//getUserPlayDataQueryStartTime := time.Now().Unix()
	rows, err := DB.Query(quarySql)
	//getUserPlayDataQueryEndTime := time.Now().Unix()

	//runtimeLogger.Output(0,fmt.Sprintf("\n\n %s \n\n",quarySql))

	//fmt.Println(fmt.Sprintf("Page:%d Task:%s Time:%s",page,"getUserPlayDataQuery",resolveSecond(getUserPlayDataQueryEndTime-getUserPlayDataQueryStartTime)))

	if err != nil {
		failureLogger.Output(0,fmt.Sprintf("Page:%d Sql:%s Error:%v",page,quarySql,err))
		return
	}

	userPlayData := new(UserPlayData)

	userPlayDatas = []UserPlayData{}

	for rows.Next() {
		rows.Scan(&userPlayData.UserId,&userPlayData.UniqueFlagCount,&userPlayData.Date)

		theTime, _ := time.ParseInLocation("2006-01", userPlayData.Date, loc)
		userPlayData.DateTime = theTime.Unix()

		//fmt.Println(*userPlayData)

		userPlayDatas = append(userPlayDatas,*userPlayData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getUserPlayDataByUserId(userId int64,userPlayDatas []UserPlayData) (userPlayData UserPlayData,isNull bool) {

	isNull = true

	for _,value := range userPlayDatas {

		if userId == value.UserId {
			
			userPlayData = value
			isNull = false
			break
		}

	}

	return userPlayData,isNull
}


func getUserDataDetailByUserId(userId int64,userDataDetails []UserPlayDataDetail) (userDataDetail []UserPlayDataDetail) {

	for _,value := range userDataDetails {

		if userId == value.UserId {
			
			userDataDetail = append(userDataDetail,value)
		}

	}

	return

}

func getSameUniqueFlagCount(userDataDetail []UserPlayDataDetail) (MaxSameUniqueFlagCount){

	uniqueFlags := []string{}

	for _,v := range userDataDetail {
		if v.UniqueFlag.Valid {
			uniqueFlags = append(uniqueFlags,v.UniqueFlag.String)
		}
	}

	sameUniqueFlagCountMap := make(map[string]int)
	for _,v := range uniqueFlags {
		_,ok := sameUniqueFlagCountMap[v]
		if ok {
			sameUniqueFlagCountMap[v] = sameUniqueFlagCountMap[v]+1
		}else {
			sameUniqueFlagCountMap[v] = 1
		}
	}

	maxSameUniqueFlagCount := MaxSameUniqueFlagCount{"",0}
	for k,v := range sameUniqueFlagCountMap {
		if v > maxSameUniqueFlagCount.Count {
			maxSameUniqueFlagCount.UniqueFlag = k
			maxSameUniqueFlagCount.Count = v
		}
	}

	return maxSameUniqueFlagCount
}

func getChannelUserLoginData(userData UserData,userPlayDatas []UserPlayData,userDataDetails []UserPlayDataDetail) (interface{}) {

	//userData.UserId = 223

	userPlayData,isNull := getUserPlayDataByUserId(userData.UserId,userPlayDatas)

	userDataDetail := getUserDataDetailByUserId(userData.UserId,userDataDetails)

	if len(userDataDetail) == 0 || isNull {
		return nil
	}

	maxSameUniqueFlagCount := getSameUniqueFlagCount(userDataDetail)

	channelUserLoginData := *new(ChannelUserLoginData)
	channelUserLoginData.ChannelId = userData.ChannelId
	channelUserLoginData.UserId = userData.UserId
	channelUserLoginData.UniqueFlagCount = userPlayData.UniqueFlagCount
	channelUserLoginData.SameUniqueFlagCount = maxSameUniqueFlagCount.Count
	channelUserLoginData.UpdateTime = time.Now().Unix()
	channelUserLoginData.Date = userPlayData.Date
	channelUserLoginData.DateTime = userPlayData.DateTime

	if id := isExistChannelUserLoginData(channelUserLoginData); id > 0 {
		channelUserLoginData.Id = id
	}

	return channelUserLoginData
	
}


func isExistChannelUserLoginData(channelUserLoginData ChannelUserLoginData) (id int64) {
	quarySql3 := fmt.Sprintf(`Select id 
	FROM gc_channel_user_login WHERE channel_id = %d AND user_id = %d AND date = '%s' LIMIT 1`,
	channelUserLoginData.ChannelId,
	channelUserLoginData.UserId,
	channelUserLoginData.Date,
	)

	row := DB.QueryRow(quarySql3)
	row.Scan(&id);

	return 
}


func saveChannelUserLoginData(channelUserLoginData interface{}) {

	if(channelUserLoginData == nil){
		return
	}

	channelUserLoginData2 := channelUserLoginData.(ChannelUserLoginData)
	var err error

	if channelUserLoginData2.Id > 0 {
		_,err = DB.Exec(
			"UPDATE gc_channel_user_login SET same_unique_flag_count = ?,unique_flag_count = ?,update_time=? WHERE id=?",
			channelUserLoginData2.SameUniqueFlagCount,
			channelUserLoginData2.UniqueFlagCount,
			channelUserLoginData2.UpdateTime,
			channelUserLoginData2.Id,
		)
	}else{
		_,err = DB.Exec(
			"insert INTO gc_channel_user_login(channel_id,user_id,same_unique_flag_count,unique_flag_count,update_time,date_time,date) values(?,?,?,?,?,?,?)",
			channelUserLoginData2.ChannelId,
			channelUserLoginData2.UserId,
			channelUserLoginData2.SameUniqueFlagCount,
			channelUserLoginData2.UniqueFlagCount,
			channelUserLoginData2.UpdateTime,
			channelUserLoginData2.DateTime,
			channelUserLoginData2.Date,
		)
	}


	if err != nil{
		pageErrorCount++
		totalErrorCount++
		//fmt.Printf("Insert failed,err:%v",err)
		failureLogger.Output(0,fmt.Sprintf(
			"ChannelId:%d UserId:%d SameUniqueFlagCount:%d UniqueFlagCount:%d UpdateTime:%d DateTime:%d Date:%s Error:%v \n\n",
			channelUserLoginData2.ChannelId,
			channelUserLoginData2.UserId,
			channelUserLoginData2.SameUniqueFlagCount,
			channelUserLoginData2.UniqueFlagCount,
			channelUserLoginData2.UpdateTime,
			channelUserLoginData2.DateTime,
			channelUserLoginData2.Date,
			err,
		))
	}else{
		pageSuccessCount++
		totalSuccessCount++
	}

	totalCount++

}



func resolveSecond(second int64) (time string) {

	minute := second/60

	hour := minute/60

	minute = minute%60

	second = second-hour*3600-minute*60

	time = fmt.Sprintf("%d:%d:%d",hour,minute,second)

	//fmt.Println(time)

	return
}