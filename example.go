package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"time"
	"workerpool/domain"
)

func main() {

	unitOfWork := &domain.UnitOfWork{
		ID:       uuid.New(),
		Code:     "sunny day",
		Work: func(ctx context.Context, payload *domain.Payload) (interface{}, error) {

			data := payload.Data.(map[string]interface{})
			time.Sleep(1 * time.Second)
			return map[string]interface{} {
				"payload_id" : payload.PayloadID.String(),
				"value" : data["value"],
			}, nil
		},
		Creation: time.Now(),
	}

	manager := domain.NewManager().With(unitOfWork)

	payloads := make([]*domain.Payload, 10)
	for i := 0; i < len(payloads); i++ {
		payloads[i] = &domain.Payload{
			Data:         map[string] interface{}{
				"value": i,
			},
			PayloadID:    uuid.New(),
			UnitOfWorkID: unitOfWork.ID,
		}
	}

	workerPool := domain.Factory(2, manager)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()


	go workerPool.Start( ctx)
	go workerPool.AddPayloads(payloads)
	go workerPool.Stop()
	for {
		select {
		case r, ok := <-workerPool.Results():
			if !ok {
				continue
			}

			if r.Err != nil{
				fmt.Errorf("Unexpected error: %v", r.Err)
			} else{
				fmt.Println(fmt.Sprintf("Value: %s", r.Value))
			}


		case <-workerPool.Done:
			fmt.Println("Result gathering completed...")
			return
		default:
		}
	}


}
