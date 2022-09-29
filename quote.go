package oqgo_ctp

import (
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	. "github.com/kkqy/ctp-go/ctp_6_3_19"
	"github.com/kkqy/util-go/slice"
	"github.com/oqgo/oqgo"
)

type Quote struct {
	CThostFtdcMdSpi
	CThostFtdcMdApi
	requestID  int
	tradingDay string

	instrumentIDexchangeIDMap map[string]string

	subscribeMutex          *sync.Mutex
	subscribedInstrumentIDs sync.Map
	subscribers             sync.Map
}

func (p *Quote) OnFrontConnected() {
	userLoginField := NewCThostFtdcReqUserLoginField()
	userLoginField.SetUserID("")
	userLoginField.SetPassword("")
	p.ReqUserLogin(userLoginField, 0)
	DeleteCThostFtdcReqUserLoginField(userLoginField)
}
func (p *Quote) OnFrontDisconnected(reason int) {

}
func (p *Quote) OnHeartBeatWarning(reason int) {

}
func (p *Quote) OnRspUserLogin(rspUserLoginField CThostFtdcRspUserLoginField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {
	p.tradingDay = rspUserLoginField.GetTradingDay()
	p.subscribedInstrumentIDs.Range(func(key, value any) bool {
		p.SubscribeMarketData([]string{key.(string)})
		return true
	})
}
func (p *Quote) OnRspUserLogout(userLogoutField CThostFtdcUserLogoutField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {

}
func (p *Quote) OnRspSubMarketData(specificInstrumentField CThostFtdcSpecificInstrumentField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {

}
func (p *Quote) OnRspQryMulticastInstrument(multicastInstrumentField CThostFtdcMulticastInstrumentField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {

}
func (p *Quote) OnRspError(rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {

}
func (p *Quote) OnRtnDepthMarketData(depthMarketDataField CThostFtdcDepthMarketDataField) {
	// 过滤没有时间戳的异常行情数据
	if depthMarketDataField.GetUpdateTime() == "" {
		return
	}
	if depthMarketDataField.GetVolume() == 0 {
		return
	}
	instrumentID := depthMarketDataField.GetInstrumentID()
	exchangeID := p.instrumentIDexchangeIDMap[instrumentID]
	tick := &oqgo.Tick{
		Symbol:       exchangeID + "." + instrumentID,
		OpenPrice:    depthMarketDataField.GetOpenPrice(),
		LowestPrice:  depthMarketDataField.GetLowestPrice(),
		HighestPrice: depthMarketDataField.GetHighestPrice(),
		ClosePrice:   depthMarketDataField.GetClosePrice(),
		AveragePrice: depthMarketDataField.GetAveragePrice(),
		LastPrice:    depthMarketDataField.GetLastPrice(),
		AskPrice1:    depthMarketDataField.GetAskPrice1(),
		AskPrice2:    depthMarketDataField.GetAskPrice2(),
		AskPrice3:    depthMarketDataField.GetAskPrice3(),
		AskPrice4:    depthMarketDataField.GetAskPrice4(),
		AskPrice5:    depthMarketDataField.GetAskPrice5(),
		AskVolume1:   int64(depthMarketDataField.GetAskVolume1()),
		AskVolume2:   int64(depthMarketDataField.GetAskVolume2()),
		AskVolume3:   int64(depthMarketDataField.GetAskVolume3()),
		AskVolume4:   int64(depthMarketDataField.GetAskVolume4()),
		AskVolume5:   int64(depthMarketDataField.GetAskVolume5()),
		BidPrice1:    depthMarketDataField.GetBidPrice1(),
		BidPrice2:    depthMarketDataField.GetBidPrice2(),
		BidPrice3:    depthMarketDataField.GetBidPrice3(),
		BidPrice4:    depthMarketDataField.GetBidPrice4(),
		BidPrice5:    depthMarketDataField.GetBidPrice5(),
		BidVolume1:   int64(depthMarketDataField.GetBidVolume1()),
		BidVolume2:   int64(depthMarketDataField.GetBidVolume2()),
		BidVolume3:   int64(depthMarketDataField.GetBidVolume3()),
		BidVolume4:   int64(depthMarketDataField.GetBidVolume4()),
		BidVolume5:   int64(depthMarketDataField.GetBidVolume5()),
		Volume:       int64(depthMarketDataField.GetVolume()),
		Turnover:     depthMarketDataField.GetTurnover(),
		OpenInterest: depthMarketDataField.GetOpenInterest(),
		TradingDay:   p.tradingDay,
		UpdateTime:   normalizeUpdateTime(depthMarketDataField.GetActionDay(), depthMarketDataField.GetUpdateTime(), depthMarketDataField.GetUpdateMillisec()),
	}
	p.subscribers.Range(func(key, value any) bool {
		channel := key.(chan<- oqgo.Tick)
		symbols := value.([]string)
		if slice.In(tick.Symbol, symbols) {
			go func(channel chan<- oqgo.Tick, tick *oqgo.Tick) {
				timer := time.NewTimer(time.Second * 3)
				select {
				case channel <- *tick:
				case <-timer.C:
					log.Println("警告:行情处理超时")
				}
			}(channel, tick)
		}
		return true
	})
}
func (p *Quote) SubscribeMarketData(instrumentIDs []string) int {
	for _, instrumentID := range instrumentIDs {
		p.subscribedInstrumentIDs.Store(instrumentID, nil)
	}
	return p.CThostFtdcMdApi.SubscribeMarketData(instrumentIDs)
}

// 获取RequestID
func (p *Quote) GetRequestID() int {
	requestID := p.requestID
	p.requestID++
	return requestID
}

// 创建一个CTP行情
func NewQuote(options map[string]string) *Quote {
	flowPath, _ := ioutil.TempDir("", "")
	os.MkdirAll(flowPath, 0777)
	p := &Quote{
		CThostFtdcMdApi:           CThostFtdcMdApiCreateFtdcMdApi(flowPath + "/"),
		tradingDay:                "",
		requestID:                 1,
		instrumentIDexchangeIDMap: make(map[string]string),
		subscribeMutex:            &sync.Mutex{},
		subscribedInstrumentIDs:   sync.Map{},
		subscribers:               sync.Map{},
	}
	p.CThostFtdcMdSpi = NewDirectorCThostFtdcMdSpi(p)
	p.RegisterSpi(p.CThostFtdcMdSpi)

	p.RegisterFront(options["md_front_address"])
	p.Init()
	return p
}
