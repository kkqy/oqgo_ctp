package oqgo_ctp

import (
	"strconv"
	"time"
)

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
func normalizeUpdateTime(actionDay string, updateTime string, millisecond int) time.Time {
	LocalTime := time.Now()
	ServerTime, _ := time.ParseInLocation("20060102 15:04:05", actionDay+" "+updateTime+"."+strconv.Itoa(millisecond), time.Local)
	if ServerTime.Hour() > 19 {
		// 夜盘的情况比较复杂
		// 先把UpdateTime和LocalTime的小时差DeltaHours求出
		// 假定最大时间误差为12小时
		// 如果超过12小时，则示为隔夜
		timeDuration1 := ServerTime.Hour()*int(time.Hour) + ServerTime.Minute()*int(time.Minute) + ServerTime.Second()*int(time.Second)
		timeDuration2 := LocalTime.Hour()*int(time.Hour) + LocalTime.Minute()*int(time.Minute) + LocalTime.Second()*int(time.Second)
		deltaHours := (timeDuration1 - timeDuration2) / int(time.Hour)
		switch true {
		case deltaHours < -12:
			// ServerTime	00:00:00
			// LocalTime	23:59:00
			// DeltaHours	-23
			// 结果分析：
			// 本地时间慢了，此行情的实际日期应该是本地日期加上一天
			return time.Date(LocalTime.Year(), LocalTime.Month(), LocalTime.Day()+1, ServerTime.Hour(), ServerTime.Minute(), ServerTime.Second(), 0, time.Local)
		case deltaHours > 12:
			// ServerTime	23:59:00
			// LocalTime	00:00:00
			// DeltaHours	23
			// 结果分析：
			// 本地时间快了（或者网络延迟），此行情的实际时间应该是本地日期减去一天
			return time.Date(LocalTime.Year(), LocalTime.Month(), LocalTime.Day()-1, ServerTime.Hour(), ServerTime.Minute(), ServerTime.Second(), 0, time.Local)
		default:
			// 其它情况下，直接用本地的日期代替
			return time.Date(LocalTime.Year(), LocalTime.Month(), LocalTime.Day(), ServerTime.Hour(), ServerTime.Minute(), ServerTime.Second(), 0, time.Local)
		}
	} else {
		// 日盘的ActionDay就是UpdateDate，所以直接返回
		return ServerTime
	}
}
