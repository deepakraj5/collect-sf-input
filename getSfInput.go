package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
)

type ExecutionResult struct {
	ExecutionArn   string `json:"executionArn"`
	ExecutionInput string `json:"executionInput"`
}

func main() {

	awsProfile := "newprod"
	ctx := context.TODO()
	var sfArn string
	var dateLimit string

	fmt.Println("Enter the SF ARN to search: ")
	fmt.Scan(&sfArn)

	fmt.Println("Enter date limit to search:")
	fmt.Scan(&dateLimit)

	// create config
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(awsProfile),
	)

	if err != nil {
		log.Panic("Error while connecting aws", err)
	}

	// create client
	client := sfn.NewFromConfig(cfg)

	var nextToken string
	var nextPage bool = true
	var iteration uint = 0

	for nextPage {
		sfExecutionOutput := fecthSFExecutions(ctx, client, sfArn, nextToken, iteration)

		if sfExecutionOutput.NextToken != nil {
			nextToken = *sfExecutionOutput.NextToken
		} else {
			nextPage = false
		}

		isNextPossible := storeInput(ctx, client, sfExecutionOutput, dateLimit, iteration)

		if !isNextPossible {
			nextPage = false
			break
		}

		iteration++
	}

}

func fecthSFExecutions(ctx context.Context, client *sfn.Client, sfArn string, nextToken string, iteration uint) sfn.ListExecutionsOutput {

	fmt.Printf("Searching through iteration: %v\n", iteration)

	var listExecutionsInput sfn.ListExecutionsInput
	if nextToken == "" {
		listExecutionsInput = sfn.ListExecutionsInput{
			StateMachineArn: &sfArn,
			MaxResults:      1000,
			StatusFilter:    types.ExecutionStatusFailed,
		}
	} else {
		listExecutionsInput = sfn.ListExecutionsInput{
			StateMachineArn: &sfArn,
			MaxResults:      1000,
			NextToken:       &nextToken,
			StatusFilter:    types.ExecutionStatusFailed,
		}
	}

	output, err := client.ListExecutions(ctx, &listExecutionsInput)

	if err != nil {
		log.Panic("Error while fetching executions ", err)
	}

	return *output
}

func fetchExecutionInput(ctx context.Context, client *sfn.Client, executionArn string) sfn.DescribeExecutionOutput {

	output, err := client.DescribeExecution(ctx, &sfn.DescribeExecutionInput{
		ExecutionArn: &executionArn,
	})

	if err != nil {
		log.Panic("Error while fetching execution input ", err)
	}

	return *output
}

func storeInput(ctx context.Context, client *sfn.Client, sfExecutionOutput sfn.ListExecutionsOutput, dateLimit string, iteration uint) bool {
	var executionsResult []ExecutionResult = make([]ExecutionResult, 0)
	var isNextPossible bool = true

	for _, execution := range sfExecutionOutput.Executions {

		executionInput := fetchExecutionInput(ctx, client, *execution.ExecutionArn)
		executionsResult = append(executionsResult, ExecutionResult{
			ExecutionArn:   *execution.ExecutionArn,
			ExecutionInput: *executionInput.Input,
		})

		dateLimitUint, _ := strconv.Atoi(dateLimit)

		fmt.Printf("execution date: %v and datelimit: %v\n", executionInput.StartDate.Unix(), int64(dateLimitUint))

		if executionInput.StartDate.Unix() < int64(dateLimitUint) {
			isNextPossible = false
			break
		}

	}

	jsonString, _ := json.Marshal(executionsResult)

	fileName := fmt.Sprintf("result%v.json", iteration)
	fmt.Println(fileName)
	os.WriteFile(fileName, jsonString, os.ModePerm)

	return isNextPossible
}
