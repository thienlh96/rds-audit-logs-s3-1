SAM                    := sam
AWS_REGION             := eu-central-1
S3_BUCKET              := personio-sar-rds-audit-logs-s3-$(AWS_REGION)
PACKAGED_TEMPLATE_FILE := packaged.yaml

.PHONY: bucket
bucket:
	aws cloudformation deploy \
		--no-fail-on-empty-changeset \
		--template-file bucket.yaml \
		--stack-name sar-bucket-rds-audit-logs-s3 \
		--parameter-overrides BucketName=$(S3_BUCKET) \
		--region $(AWS_REGION)

.PHONY: build
build:
	$(SAM) build

.PHONY: package
package: build
	$(SAM) package --s3-bucket $(S3_BUCKET) --region $(AWS_REGION) --output-template-file $(PACKAGED_TEMPLATE_FILE)

.PHONY: publish
publish: package
	$(SAM) publish --template-file $(PACKAGED_TEMPLATE_FILE)
