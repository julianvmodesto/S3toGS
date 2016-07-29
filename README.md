# S3toGS
Tiny utility to sync an AWS S3 directory to GCP GS directory
by locally downloading and uploading one file at a time.

Replicates `gsutil rsync -d -r gs://my-gs-bucket s3://my-s3-bucket`,
but supports S3 user-specific directories.

Install AWS CLI and GCP SDK and set up your respective credentials.

# Alternative
I highly recommend using https://github.com/ncw/rclone instead. Fast sync utility for multiple clouds written in Go. Supports S3  user-specific directories.
