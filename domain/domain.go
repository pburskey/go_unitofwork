package unitofwork

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)


type UnitOfWorkCode string
//type UnitOfWorkMetaData map[string]interface{}

type Command func(ctx context.Context, payload *Payload) (interface{}, error)

type UnitOfWork struct{
	ID	uuid.UUID
	Code UnitOfWorkCode
	Work Command
	Creation time.Time
}

type UnitOfWorkResult struct {
	Value      		interface{}
	PayloadID		uuid.UUID
	Err        		error
	UnitOfWorkID	uuid.UUID
}

type Payload struct{
	Data			interface{}
	PayloadID		uuid.UUID
	UnitOfWorkID	uuid.UUID
}

func (unitOfWork *UnitOfWork) process(ctx context.Context, payload *Payload) (error, *UnitOfWorkResult) {
	
	value, err := unitOfWork.Work(ctx, payload)
	unitOfWorkResult := &UnitOfWorkResult{
		Value:        value,
		PayloadID:      payload.PayloadID,
		Err:          err,
		UnitOfWorkID: unitOfWork.ID,
	}
	return err, unitOfWorkResult

}

type UnitOfWorkManager struct {
	unitsOfWork map[string]*UnitOfWork
}

func NewManager() *UnitOfWorkManager{
	return &UnitOfWorkManager{
		unitsOfWork: make(map[string]*UnitOfWork, 0),
	}
}

func (manager *UnitOfWorkManager) With(unitOfWork *UnitOfWork) *UnitOfWorkManager{
	manager.unitsOfWork[unitOfWork.ID.String()] = unitOfWork
	return manager
}

func(manager UnitOfWorkManager) GetUsingID(uuid uuid.UUID) *UnitOfWork{
	if unitOfWork, ok := manager.unitsOfWork[uuid.String()]; ok{
		return unitOfWork
	}
	return nil
}


func worker(id string, manager *UnitOfWorkManager, ctx context.Context, wg *sync.WaitGroup, payloads <-chan *Payload, unitOfWorkResults chan<- *UnitOfWorkResult) {
	defer wg.Done()
	threadID := fmt.Sprintf("Thread[%s]", id)
	fmt.Println(threadID + " starting...")
	for {
		select {
		case payload, ok := <-payloads:
			if !ok {
				fmt.Println(threadID + " not ok, thread complete...")
				return
			}
			fmt.Println(threadID + fmt.Sprintf(" staring work on payload: %v...", payload.PayloadID))
			unitOfWork := manager.GetUsingID(payload.UnitOfWorkID)

			// fan-in job execution multiplexing results into the results channel
			value, err := unitOfWork.Work(ctx, payload)
			result := &UnitOfWorkResult{
				Value:        	value,
				PayloadID:     	payload.PayloadID,
				Err:          	err,
				UnitOfWorkID: 	unitOfWork.ID,
			}
			unitOfWorkResults <- result
			fmt.Println(threadID + fmt.Sprintf(" completed work on payload: %v...", payload.PayloadID))
		case <-ctx.Done():
			fmt.Printf(threadID + " cancelled. Error detail: %v\n", ctx.Err())
			unitOfWorkResults <- &UnitOfWorkResult{
				Err: ctx.Err(),
			}
			return
		}
	}
}

type UnitOfWorkServer struct {
	workersCount 			int
	incomingPayloads        chan *Payload
	outgoingPayloads      	chan *UnitOfWorkResult
	Done         			chan struct{}
	manager 				*UnitOfWorkManager
}

func Factory(count int, manager *UnitOfWorkManager) UnitOfWorkServer {
	return UnitOfWorkServer{
		workersCount: 	count,
		incomingPayloads:       make(chan *Payload, count),
		outgoingPayloads:      	make(chan *UnitOfWorkResult, count),
		Done:         	make(chan struct{}),
		manager: 		manager,
	}
}

func (wp UnitOfWorkServer) Start( ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < wp.workersCount; i++ {
		wg.Add(1)
		go worker(fmt.Sprintf("%v", i), wp.manager, ctx, &wg, wp.incomingPayloads, wp.outgoingPayloads)
	}

	wg.Wait()
	//close(wp.Done)
	//close(wp.results)
}

func (wp UnitOfWorkServer) Results() <-chan *UnitOfWorkResult {
	return wp.outgoingPayloads
}

func (wp UnitOfWorkServer) AddPayloadsAndSignalNoMoreWork(payloads []*Payload) {
	wp.AddPayloads(payloads)
	wp.NoMoreWork()
}

func (wp UnitOfWorkServer) AddPayloads(payloads []*Payload) {
	for i, _ := range payloads {
		wp.incomingPayloads <- payloads[i]
	}
}

func (wp UnitOfWorkServer) Stop() {

	wp.NoMoreWork()
	wp.NoMoreResults()

	close(wp.Done)
}

func (wp UnitOfWorkServer) NoMoreWork() {
	wp.WaitForCompletionOfWork()
	select {
	case _, ok := <-wp.incomingPayloads:
		if ok {
			close(wp.incomingPayloads)
		}
	default:
	}


}

func (wp UnitOfWorkServer) NoMoreResults() {
	wp.WaitForDigestionOfResults()
	select {
	case _, ok := <-wp.outgoingPayloads:
		if ok {
			close(wp.outgoingPayloads)
		}
	default:
	}
}
func (wp UnitOfWorkServer) WaitForCompletionOfWork() {
	for{
		time.Sleep(1 * time.Second)
		if !wp.HasPayloadsRemainingToBeWorked(){
			return
		}
	}
}

func (wp UnitOfWorkServer) WaitForDigestionOfResults() {
	for{
		time.Sleep(1 * time.Second)
		if !wp.HasRemainingResults(){
			return
		}
	}
}

func (wp UnitOfWorkServer) HasPayloadsRemainingToBeWorked() bool{
	return len(wp.incomingPayloads) > 0
}

func (wp UnitOfWorkServer) HasRemainingResults() bool{
	return len(wp.outgoingPayloads) > 0
}