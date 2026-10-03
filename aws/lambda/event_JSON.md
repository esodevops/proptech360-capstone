# Event JSON:

```json
{
  "Records": [
    {
      "eventSource": "aws:s3",
      "eventName": "ObjectCreated:Put",
      "awsRegion": "eu-north-1",
      "s3": {
        "bucket": {
          "name": "proptech360-bucket-800557027629"
        },
        "object": {
          "key": "raw/properties.csv"
        }
      }
    }
  ]
}
```