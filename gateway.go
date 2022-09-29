package oqgo_ctp

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kkqy/asynchan"
	. "github.com/kkqy/ctp-go/ctp_6_3_19"
	"github.com/oqgo/oqgo"
)

type CTP struct {
	quote  *Quote
	trader *Trader

	userID, brokerID string
}

func (p *CTP) GetAllSubjects() ([]oqgo.Subject, error) {
	requestID := int(p.trader.requestID.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	asyncResponse := asynchan.New(ctx)
	p.trader.asyncResponseMap.Store(requestID, asyncResponse)
	defer p.trader.asyncResponseMap.Delete(requestID)
	qryInstrumentField := NewCThostFtdcQryInstrumentField()
	defer DeleteCThostFtdcQryInstrumentField(qryInstrumentField)
	p.trader.ReqQryInstrument(qryInstrumentField, requestID)
	results, err := asyncResponse.AwaitAll()
	checkError(err)
	subjects := make([]oqgo.Subject, 0)
	for _, result := range results {
		subjects = append(subjects, *result.(*oqgo.Subject))
	}
	return subjects, nil
}
func (p *CTP) GetPositions(symbol string) ([]oqgo.Position, error) {
	splitedSymbol := strings.Split(symbol, ".")
	requestID := int(p.trader.requestID.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	asyncResponse := asynchan.New(ctx)
	p.trader.asyncResponseMap.Store(requestID, asyncResponse)
	defer p.trader.asyncResponseMap.Delete(requestID)
	qryInvestorPositionField := NewCThostFtdcQryInvestorPositionField()
	defer DeleteCThostFtdcQryInvestorPositionField(qryInvestorPositionField)
	qryInvestorPositionField.SetInstrumentID(splitedSymbol[1])
	p.trader.ReqQryInvestorPosition(qryInvestorPositionField, requestID)
	results, err := asyncResponse.AwaitAll()
	checkError(err)
	positions := make([]oqgo.Position, 0)
	for _, result := range results {
		positions = append(positions, *result.(*oqgo.Position))
	}
	return positions, nil
}
func (p *CTP) Subscribe(chanel chan<- oqgo.Tick, symbols []string) error {
	instrumentIDs := make([]string, 0)
	for _, symbol := range symbols {
		value, _ := p.quote.subscribedInstrumentIDs.LoadOrStore(symbol, &atomic.Int32{})
		value.(*atomic.Int32).Add(1)
		splitedSymbol := strings.Split(symbol, ".")
		instrumentIDs = append(instrumentIDs, splitedSymbol[1])
		p.quote.instrumentIDexchangeIDMap[splitedSymbol[1]] = splitedSymbol[0]
	}
	p.quote.SubscribeMarketData(instrumentIDs)
	p.quote.subscribers.Store(chanel, symbols)
	return nil
}
func (p *CTP) SendOrder(symbol string, price float64, volume int, direction oqgo.Direction, offset oqgo.Offset) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return p.trader.awaitOne(ctx, func(requestID int) {
		inputOrderField := NewCThostFtdcInputOrderField()
		defer DeleteCThostFtdcInputOrderField(inputOrderField)
		splitedSymbol := strings.Split(symbol, ".")
		exchangeID, instrumentID := splitedSymbol[0], splitedSymbol[1]
		inputOrderField.SetRequestID(requestID)
		inputOrderField.SetBrokerID(p.brokerID)
		inputOrderField.SetInvestorID(p.userID)

		inputOrderField.SetInstrumentID(instrumentID)
		inputOrderField.SetExchangeID(exchangeID)

		inputOrderField.SetLimitPrice(price)
		inputOrderField.SetVolumeTotalOriginal(volume)
		inputOrderField.SetOrderPriceType(THOST_FTDC_OPT_LimitPrice)
		switch direction {
		case oqgo.DirectionLong:
			inputOrderField.SetDirection(THOST_FTDC_D_Buy)
		case oqgo.DirectionShort:
			inputOrderField.SetDirection(THOST_FTDC_D_Sell)
		}

		switch offset {
		case oqgo.OffsetOpen:
			inputOrderField.SetCombOffsetFlag(string(THOST_FTDC_OF_Open))
		case oqgo.OffsetClose:
			inputOrderField.SetCombOffsetFlag(string(THOST_FTDC_OF_Close))
		}
		inputOrderField.SetCombHedgeFlag(string(THOST_FTDC_HF_Speculation))
		inputOrderField.SetForceCloseReason(THOST_FTDC_FCC_NotForceClose)
		inputOrderField.SetContingentCondition(THOST_FTDC_CC_Immediately)
		inputOrderField.SetTimeCondition(THOST_FTDC_TC_GFD)
		inputOrderField.SetVolumeCondition(THOST_FTDC_VC_AV)

		p.trader.ReqOrderInsert(inputOrderField, requestID)
	})
}
func (p *CTP) QueryOrders() ([]oqgo.Order, error) {
	// ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	// defer cancel()
	// requestID := int(p.trader.requestID.Add(1))
	// qryOrderField := NewCThostFtdcQryOrderField()
	// defer DeleteCThostFtdcQryOrderField(qryOrderField)
	// p.trader.ReqQryOrder(qryOrderField, requestID)

	return nil, nil
}
func New(options map[string]string) *CTP {
	p := &CTP{
		quote:    NewQuote(options),
		trader:   NewTrader(options),
		userID:   options["user_id"],
		brokerID: options["broker_id"],
	}
	return p
}
