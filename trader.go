package oqgo_ctp

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"

	"github.com/kkqy/asynchan"
	. "github.com/kkqy/ctp-go/ctp_6_3_19"
	"github.com/oqgo/oqgo"
)

type Trader struct {
	CThostFtdcTraderSpi
	CThostFtdcTraderApi

	brokerID, userProductionInfo, authCode, appID string
	userID, password                              string

	tradingDay string
	requestID  *atomic.Int32

	asyncResponseMap sync.Map
}

func (p *Trader) createRequest(ctx context.Context, requestID int) *asynchan.Asynchan {
	asyncResponse := asynchan.New(ctx)
	p.asyncResponseMap.Store(requestID, asyncResponse)
	return asyncResponse
}
func (p *Trader) awaitOne(ctx context.Context, reqFunc func(requestID int)) (interface{}, error) {
	requestID := int(p.requestID.Add(1))
	asyncResponse := p.createRequest(ctx, requestID)
	defer p.asyncResponseMap.Delete(requestID)
	reqFunc(requestID)
	return asyncResponse.AwaitOne()
}
func (p *Trader) awaitAll(ctx context.Context, reqFunc func(requestID int)) ([]interface{}, error) {
	requestID := int(p.requestID.Add(1))
	asyncResponse := p.createRequest(ctx, requestID)
	defer p.asyncResponseMap.Delete(requestID)
	reqFunc(requestID)
	return asyncResponse.AwaitAll()
}

func (p *Trader) OnFrontConnected() {
	reqAuthenticateField := NewCThostFtdcReqAuthenticateField()
	reqAuthenticateField.SetBrokerID(p.brokerID)
	reqAuthenticateField.SetUserProductInfo(p.userProductionInfo)
	reqAuthenticateField.SetAuthCode(p.authCode)
	reqAuthenticateField.SetAppID(p.appID)
	p.ReqAuthenticate(reqAuthenticateField, int(p.requestID.Add(1)))
	DeleteCThostFtdcReqAuthenticateField(reqAuthenticateField)
}
func (p *Trader) OnFrontDisconnected(nReason int) {

}
func (p *Trader) OnHeartBeatWarning(nTimeLapse int) {

}
func (p *Trader) OnRspAuthenticate(rspAuthenticateField CThostFtdcRspAuthenticateField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {
	userLoginField := NewCThostFtdcReqUserLoginField()
	userLoginField.SetUserID(p.userID)
	userLoginField.SetPassword(p.password)
	userLoginField.SetBrokerID(p.brokerID)
	p.ReqUserLogin(userLoginField, 0)
	DeleteCThostFtdcReqUserLoginField(userLoginField)
}
func (p *Trader) OnRspUserLogin(rspUserLoginField CThostFtdcRspUserLoginField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {
	if rspInfoField.GetErrorID() == 0 {
		p.tradingDay = rspUserLoginField.GetTradingDay()
		// 结算结果确认
		settlementInfoConfirmField := NewCThostFtdcSettlementInfoConfirmField()
		defer DeleteCThostFtdcSettlementInfoConfirmField(settlementInfoConfirmField)
		settlementInfoConfirmField.SetBrokerID(p.brokerID)
		settlementInfoConfirmField.SetInvestorID(p.userID)
		p.ReqSettlementInfoConfirm(settlementInfoConfirmField, int(p.requestID.Add(1)))
	}
}
func (p *Trader) OnRspSettlementInfoConfirm(settlementInfoConfirmField CThostFtdcSettlementInfoConfirmField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {

}
func (p *Trader) OnRspQryInstrument(instrumentField CThostFtdcInstrumentField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {
	value, ok := p.asyncResponseMap.Load(requestID)
	if ok {
		asyncResponse := value.(*asynchan.Asynchan)
		asyncResponse.SetResult(&oqgo.Subject{
			Symbol: instrumentField.GetExchangeID() + "." + instrumentField.GetInstrumentID(),
		})
		if isLast {
			asyncResponse.Close()
		}
	}
}
func (p *Trader) OnRtnInstrumentStatus(instrumentStatusField CThostFtdcInstrumentStatusField) {

}
func (p *Trader) OnRspQryInvestorPosition(investorPositionField CThostFtdcInvestorPositionField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {
	value, ok := p.asyncResponseMap.Load(requestID)
	if ok {
		asyncResponse := value.(*asynchan.Asynchan)
		if investorPositionField.Swigcptr() != 0 {
			asyncResponse.SetResult(&oqgo.Position{
				Symbol:    investorPositionField.GetInstrumentID(),
				Direction: oqgo.Direction(investorPositionField.GetPosiDirection()),
			})
		}
		if isLast {
			asyncResponse.Close()
		}
	}
}
func (p *Trader) OnRspOrderInsert(inputOrderField CThostFtdcInputOrderField, rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {
	value, ok := p.asyncResponseMap.Load(requestID)
	if ok {
		asyncResponse := value.(*asynchan.Asynchan)
		asyncResponse.SetError(errors.New(rspInfoField.GetErrorMsg()))
		if isLast {
			asyncResponse.Close()
		}
	}
}
func (p *Trader) OnRspError(rspInfoField CThostFtdcRspInfoField, requestID int, isLast bool) {
	fmt.Printf("发生错误:%d\n", rspInfoField.GetErrorID())
}
func (p *Trader) OnRtnOrder(orderField CThostFtdcOrderField) {

}
func (p *Trader) OnRtnTrade(tradeField CThostFtdcTradeField) {

}
func (p *Trader) OnErrRtnOrderInsert(inputOrderField CThostFtdcInputOrderField, rspInfoField CThostFtdcRspInfoField) {

}
func NewTrader(options map[string]string) *Trader {
	flowPath, _ := ioutil.TempDir("", "")
	os.MkdirAll(flowPath, 0777)
	p := &Trader{
		CThostFtdcTraderApi: CThostFtdcTraderApiCreateFtdcTraderApi(flowPath + "/"),
		tradingDay:          "",
		requestID:           &atomic.Int32{},
		asyncResponseMap:    sync.Map{},
	}
	p.CThostFtdcTraderSpi = NewDirectorCThostFtdcTraderSpi(p)
	p.RegisterSpi(p.CThostFtdcTraderSpi)

	p.brokerID = options["broker_id"]
	p.userProductionInfo = options["user_production_info"]
	p.authCode = options["auth_code"]
	p.appID = options["app_id"]
	p.userID = options["user_id"]
	p.password = options["password"]

	p.RegisterFront(options["trader_front_address"])
	p.Init()
	return p
}
