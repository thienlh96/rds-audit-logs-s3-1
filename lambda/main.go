package main

import (
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/kelseyhightower/envconfig"
	"rdsauditlogss3/internal/database"
	"rdsauditlogss3/internal/logcollector"
	"rdsauditlogss3/internal/parser"
	"rdsauditlogss3/internal/processor"
	"rdsauditlogss3/internal/s3writer"

	log "github.com/sirupsen/logrus"
)

// HandlerConfig holds the configuration for the lambda function
type HandlerConfig struct {
	RdsInstanceIdentifier string `envconfig:"RDS_INSTANCE_IDENTIFIER" required:"true" desc:"Identifier of the RDS instance"`
	S3BucketName          string `envconfig:"S3_BUCKET_NAME" required:"true" desc:"Name of the bucket to write logs to"`
	DynamoDbTableName     string `envconfig:"DYNAMODB_TABLE_NAME" required:"true" desc:"DynamoDb table name"`
	AwsRegion             string `envconfig:"AWS_REGION" required:"true" desc:"AWS region"`
	Debug                 bool   `envconfig:"DEBUG" required:"true" desc:"Enable debug mode."`
	LogPrefix             string   `envconfig:"LogPrefix" required:"true" desc:"rds log file prefix."`
	FolderS3              string   `envconfig:"FolderS3" required:"true" desc:"s3 folder saved log."`
}

type lambdaHandler struct {
	processor *processor.Processor
}

// Handler is the handler registered as the lambda function handler
func (lh *lambdaHandler) Handler() error {
	err := lh.processor.Process()
	if err != nil {
		log.WithError(err).Errorf("Error in Lambda function")
		return fmt.Errorf("error in Lambda function")
	}
	return nil
}

func main() {
	var c HandlerConfig
	// Dev env
	// c.AwsRegion = "us-east-1"
	// c.DynamoDbTableName = "rds-cache-timestamp"
	// c.S3BucketName = "cf-templates-6w08wj2iqgqp-us-east-1"
	// c.RdsInstanceIdentifier = "mothership-instance-1"
	// c.FolderS3 = "audit_log"
	// c.LogPrefix = "audit/audit"
	// c.Debug = true

	err := envconfig.Process("", &c)
	if err != nil {
		log.WithError(err).Fatal("Error parsing configuration")
	}

	if c.Debug {
		log.SetLevel(log.DebugLevel)
	}

	// Initialize AWS session
	sessionConfig := &aws.Config{
		Region: aws.String(c.AwsRegion),
	}

	sess := session.New(sessionConfig)

	// Dev session
	// sess, err := session.NewSessionWithOptions(session.Options{
	// 	Profile: "605272924796_wl_stag_infraops_4h_permset",
	// 	Config:  aws.Config{Region: aws.String("us-east-1")},
	// 	SharedConfigState: session.SharedConfigEnable,
	// })
	// if err!= nil {
	// 	fmt.Println(err)
	// }
	// Create & start lambda handler
	lh := &lambdaHandler{
		processor: processor.NewProcessor(
			database.NewDynamoDb(
				dynamodb.New(sess),
				c.DynamoDbTableName,
			),
			logcollector.NewRdsLogCollector(
				rds.New(sess),
				logcollector.NewAWSHttpClient(sess),
				c.AwsRegion,
				c.RdsInstanceIdentifier,
				"mysql",
				c.LogPrefix,
			),
			s3writer.NewS3Writer(
				s3manager.NewUploader(sess),
				c.S3BucketName,
				fmt.Sprintf("%s/%s", c.RdsInstanceIdentifier, c.FolderS3),
			),
			parser.NewAuditLogParser(),
			c.RdsInstanceIdentifier,
		),
	}
	// dev main
	// lh.Handler()
	lambda.Start(lh.Handler)
}
