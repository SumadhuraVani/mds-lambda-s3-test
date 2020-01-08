using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Xunit;
using Amazon.Lambda.Core;
using Amazon.Lambda.TestUtilities;

using MDS_Lambda_S3;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon;
using Amazon.S3.Util;
using Amazon.S3.Model;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace MDS_Lambda_S3.Tests
{
    public class FunctionTest
    {
        [Fact]
        public async Task TestSourceFunctionAsyncRSMapIssue()
       {
            // Invoke the lambda function and confirm the string was upper cased.
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(RegionEndpoint.USEast1);
            List<KeySchemaElement> schema = new List<KeySchemaElement>
                {
    new KeySchemaElement
    {
        AttributeName = "guid", KeyType = "HASH"
    },
    new KeySchemaElement
    {
        AttributeName = "Data", KeyType = "RANGE"
    }
                };

            // Define key attributes:
            //  The key attributes "Author" and "Title" are string types
            List<AttributeDefinition> definitions = new List<AttributeDefinition>
                {
    new AttributeDefinition
    {
        AttributeName = "guid", AttributeType = "S"
    },
    new AttributeDefinition
    {
        AttributeName = "Data", AttributeType = "S"
    }
                };

            // Define table throughput:
            //  Table has capacity of 20 reads and 50 writes
            ProvisionedThroughput throughput = new ProvisionedThroughput
            {
                ReadCapacityUnits = 20,
                WriteCapacityUnits = 50
            };

            // Configure the CreateTable request
            CreateTableRequest request = new CreateTableRequest
            {
                TableName = "MDSSourceDynamoDBTestDat1",
                KeySchema = schema,
                ProvisionedThroughput = throughput,
                AttributeDefinitions = definitions
            };


            // View new table properties
            var tableDescription = await client.CreateTableAsync(request);
            Console.WriteLine("Table name: {0}", tableDescription.TableDescription.TableName);
            Console.WriteLine("Creation time: {0}", tableDescription.TableDescription.CreationDateTime);
            Console.WriteLine("Item count: {0}", tableDescription.TableDescription.ItemCount);
            Console.WriteLine("Table size (bytes): {0}", tableDescription.TableDescription.TableSizeBytes);
            Console.WriteLine("Table status: {0}", tableDescription.TableDescription.TableStatus);
            //dbList.Add()

            Table Catolog = Table.LoadTable(client, tableDescription.TableDescription.TableName);
            string status = null;
            // Let us wait until table is created. Call DescribeTable.
            do
            {
                System.Threading.Thread.Sleep(5000); // Wait 5 seconds.
                try
                {
                    var res = await client.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = "MDSSourceDynamoDBTestDat1"
                    });

                    Console.WriteLine("Table name: {0}, status: {1}",
                              res.Table.TableName,
                              res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }
                catch (ResourceNotFoundException)
                {
                    // DescribeTable is eventually consistent. So you might
                    // get resource not found. So we handle the potential exception.
                }
            } while (status != "ACTIVE");
            Console.WriteLine("\n*** listing tables ***");

            IAmazonS3 s3Client = new AmazonS3Client(RegionEndpoint.USEast1);
            string inputData = "#BUSINESS_DATE|CURRENCY|CREDIT_TYPE|RATINGS|TODAY|DAY_1|DAYS_7|DAYS_30|DAYS_90|DAYS_365|YEARS_2|YEARS_3|LOAD_DATE\n20190828|USD|CORPS|AAA|17.506131099768|17.499652220154|17.689977433049|17.612596564917|17.531741482981|16.721663421274|22.812501511208|28.591981044712|08/29/2019";
            var destinationBucketName = "spgmi-dest-buck-test";
            var bucketName = "spgi-mds-data-dev-test2".ToLower();
            var key = "MDR_EQUITY_PDR_ZSCORE_INCR_20191217032542.txt";

            // Create a bucket an object to setup a test data.
            await s3Client.PutBucketAsync(destinationBucketName);
            try
            {
                await s3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentBody = inputData
                });

                // Setup the S3 event object that S3 notifications would create with the fields used by the Lambda function.
                var s3Event = new S3Event
                {
                    Records = new List<S3EventNotification.S3EventNotificationRecord>
                    {
                        new S3EventNotification.S3EventNotificationRecord
                        {
                            S3 = new S3EventNotification.S3Entity
                            {
                                Bucket = new S3EventNotification.S3BucketEntity {Name = bucketName },
                                Object = new S3EventNotification.S3ObjectEntity {Key = key }
                            }
                        }
                    }
                };
                var context = new TestLambdaContext();
                // Invoke the lambda function and confirm the content type was returned.
                var function = new Function(s3Client);
                var contentType = await function.FunctionHandler(s3Event, context);

                Assert.Equal("text/plain", contentType);

            }
            catch (Exception ex)
            {

            }
        }

        [Fact]
        public async Task TestSourceFunctionAsyncRSMapSuccess()
        {
            // Invoke the lambda function and confirm the string was upper cased.
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(RegionEndpoint.USEast1);
            List<KeySchemaElement> schema = new List<KeySchemaElement>
                {
    new KeySchemaElement
    {
        AttributeName = "guid", KeyType = "HASH"
    },
    new KeySchemaElement
    {
        AttributeName = "Data", KeyType = "RANGE"
    }
                };

            // Define key attributes:
            //  The key attributes "Author" and "Title" are string types
            List<AttributeDefinition> definitions = new List<AttributeDefinition>
                {
    new AttributeDefinition
    {
        AttributeName = "guid", AttributeType = "S"
    },
    new AttributeDefinition
    {
        AttributeName = "Data", AttributeType = "S"
    }
                };

            // Define table throughput:
            //  Table has capacity of 20 reads and 50 writes
            ProvisionedThroughput throughput = new ProvisionedThroughput
            {
                ReadCapacityUnits = 20,
                WriteCapacityUnits = 50
            };

            // Configure the CreateTable request
            CreateTableRequest request = new CreateTableRequest
            {
                TableName = "MDSSourceDynamoDBTestDat1",
                KeySchema = schema,
                ProvisionedThroughput = throughput,
                AttributeDefinitions = definitions
            };


            // View new table properties
            var tableDescription = await client.CreateTableAsync(request);
            Console.WriteLine("Table name: {0}", tableDescription.TableDescription.TableName);
            Console.WriteLine("Creation time: {0}", tableDescription.TableDescription.CreationDateTime);
            Console.WriteLine("Item count: {0}", tableDescription.TableDescription.ItemCount);
            Console.WriteLine("Table size (bytes): {0}", tableDescription.TableDescription.TableSizeBytes);
            Console.WriteLine("Table status: {0}", tableDescription.TableDescription.TableStatus);
            //dbList.Add()

            Table Catolog = Table.LoadTable(client, tableDescription.TableDescription.TableName);
            string status = null;
            // Let us wait until table is created. Call DescribeTable.
            do
            {
                System.Threading.Thread.Sleep(5000); // Wait 5 seconds.
                try
                {
                    var res = await client.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = "MDSSourceDynamoDBTestDat1"
                    });

                    Console.WriteLine("Table name: {0}, status: {1}",
                              res.Table.TableName,
                              res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }
                catch (ResourceNotFoundException)
                {
                    // DescribeTable is eventually consistent. So you might
                    // get resource not found. So we handle the potential exception.
                }
            } while (status != "ACTIVE");
            Console.WriteLine("\n*** listing tables ***");

            IAmazonS3 s3Client = new AmazonS3Client(RegionEndpoint.USEast1);
            string inputData = "#BUSINESS_DATE|CURRENCY|CREDIT_TYPE|RATINGS|TODAY|DAY_1|DAYS_7|DAYS_30|DAYS_90|DAYS_365|YEARS_2|YEARS_3|LOAD_DATE\n20190828|USD|CORP|AAA|17.506131099768|17.499652220154|17.689977433049|17.612596564917|17.531741482981|16.721663421274|22.812501511208|28.591981044712|08/29/2019";
            var destinationBucketName = "spgmi-dest-buck-test";
            var bucketName = "spgi-mds-data-dev-test2".ToLower();
            var key = "MDR_EQUITY_PDR_ZSCORE_INCR_20191217032542.txt";

            // Create a bucket an object to setup a test data.
            await s3Client.PutBucketAsync(destinationBucketName);
            try
            {
                await s3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentBody = inputData
                });

                // Setup the S3 event object that S3 notifications would create with the fields used by the Lambda function.
                var s3Event = new S3Event
                {
                    Records = new List<S3EventNotification.S3EventNotificationRecord>
                    {
                        new S3EventNotification.S3EventNotificationRecord
                        {
                            S3 = new S3EventNotification.S3Entity
                            {
                                Bucket = new S3EventNotification.S3BucketEntity {Name = bucketName },
                                Object = new S3EventNotification.S3ObjectEntity {Key = key }
                            }
                        }
                    }
                };
                var context = new TestLambdaContext();
                // Invoke the lambda function and confirm the content type was returned.
                var function = new Function(s3Client);
                var contentType = await function.FunctionHandler(s3Event, context);

                Assert.Equal("text/plain", contentType);

            }
            catch (Exception ex)
            {

            }
        }

    }
}
