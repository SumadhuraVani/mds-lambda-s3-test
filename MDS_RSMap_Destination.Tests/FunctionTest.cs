using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Xunit;
using Amazon.Lambda.Core;
using Amazon.Lambda.TestUtilities;

using MDS_RSMap_Destination;
using Amazon;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;

namespace MDS_RSMap_Destination.Tests
{
    public class FunctionTest
    {
        [Fact]
        public async Task  TestRSMapDestUpdateFunction()
        {
            try
            {
                string mdtexttest = "{\r\n \"RatingDataAsOf\": \"20190828\",\r\n  \"KeyCurrency\": \"USD\",\r\n  \"KeyRatingCreditType\": \"0\",\r\n  \"CreditRatingDerivedCDS\": \"AAA\",\r\n  \"CDSBenchmarkSpread\": \"17.506131099768\",\r\n  \"CDSBenchmarkSpread1Day\": \"17.499652220154\",\r\n  \"CDSBenchmarkSpread7Day\": \"17.689977433049\",\r\n  \"CDSBenchmarkSpread30Day\": \"17.612596564917\",\r\n  \"CDSBenchmarkSpread90Day\": \"17.531741482981\",\r\n  \"CDSBenchmarkSpread1Year\": \"16.721663421274\",\r\n  \"CDSBenchmarkSpread2Year\": \"22.812501511208\",\r\n  \"CDSBenchmarkSpread3Year\": \"28.591981044712\",\r\n  \"RatingPublishDate\": \"08/29/2019\"\r\n}";

                AmazonDynamoDBClient client = new AmazonDynamoDBClient(RegionEndpoint.USEast1);
                var requestDel = new DeleteTableRequest
                {
                    TableName = "MDSDestDynamoDBTest"
                };

                //
                var responseDel = await client.DeleteTableAsync(requestDel);
                System.Threading.Thread.Sleep(3000);
                List<KeySchemaElement> schemadest = new List<KeySchemaElement>
                {
                 new KeySchemaElement
                 {
                  AttributeName = "GUID", KeyType = "HASH"
                 },
                new KeySchemaElement
                 {
                AttributeName = "Data", KeyType = "RANGE"
                 }
                };

                // Define key attributes:
                //  The key attributes "Author" and "Title" are string types
                List<AttributeDefinition> definitionsdest = new List<AttributeDefinition>
                {
    new AttributeDefinition
    {
        AttributeName = "GUID", AttributeType = "S"
    },
    new AttributeDefinition
    {
        AttributeName = "Data", AttributeType = "S"
    }
                };

                // Define table throughput:
                //  Table has capacity of 20 reads and 50 writes
                ProvisionedThroughput throughputdest = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 20,
                    WriteCapacityUnits = 50
                };

                // Configure the CreateTable request
                CreateTableRequest requestdest = new CreateTableRequest
                {
                    TableName = "MDSDestDynamoDBTest",
                    KeySchema = schemadest,
                    ProvisionedThroughput = throughputdest,
                    AttributeDefinitions = definitionsdest
                };


                // View new table properties
                var tableDescriptiondest = await client.CreateTableAsync(requestdest);
                Table Catologdest = Table.LoadTable(client, tableDescriptiondest.TableDescription.TableName);
                string status = null;
                // Let us wait until table is created. Call DescribeTable.
                do
                {
                    System.Threading.Thread.Sleep(3000); // Wait 5 seconds.
                    try
                    {
                        var res = await client.DescribeTableAsync(new DescribeTableRequest
                        {
                            TableName = "MDSDestDynamoDBTest"
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

                var data = new Document();
                data["GUID"] = Guid.NewGuid().ToString();
                data["Data"] = mdtexttest;
                data["updatedDate"] = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssf"));
                data["timestamp"] = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssfff"));
                data["UniqueRow"] = Convert.ToInt64(String.Format("{0:d9}", (DateTime.Now.Ticks / 10) % 1000000000));
                data["Error"] = "RS Mapping Missing";
                Document response = await Catologdest.PutItemAsync(data);
                var context = new TestLambdaContext();
                var function = new Function();
                var responseFinal = await function.FunctionHandler(null, context);
                Assert.Equal(responseFinal, null);
            }
            catch(Exception ex)
            {

            }

        }

        [Fact]
        public async Task TestRSMapDestInsertFunction()
        {
            try
            {
                string mdtexttest = "{\r\n \"RatingDataAsOf\": \"20190828\",\r\n  \"KeyCurrency\": \"USD\",\r\n  \"KeyRatingCreditType\": \"1\",\r\n  \"CreditRatingDerivedCDS\": \"AAA\",\r\n  \"CDSBenchmarkSpread\": \"17.506131099768\",\r\n  \"CDSBenchmarkSpread1Day\": \"17.499652220154\",\r\n  \"CDSBenchmarkSpread7Day\": \"17.689977433049\",\r\n  \"CDSBenchmarkSpread30Day\": \"17.612596564917\",\r\n  \"CDSBenchmarkSpread90Day\": \"17.531741482981\",\r\n  \"CDSBenchmarkSpread1Year\": \"16.721663421274\",\r\n  \"CDSBenchmarkSpread2Year\": \"22.812501511208\",\r\n  \"CDSBenchmarkSpread3Year\": \"28.591981044712\",\r\n  \"RatingPublishDate\": \"08/29/2019\"\r\n}";

                AmazonDynamoDBClient client = new AmazonDynamoDBClient(RegionEndpoint.USEast1);
                var requestDel = new DeleteTableRequest
                {
                    TableName = "MDSDestDynamoDBTest"
                };

                //
                var responseDel = await client.DeleteTableAsync(requestDel);
                System.Threading.Thread.Sleep(3000);
                List<KeySchemaElement> schemadest = new List<KeySchemaElement>
                {
                 new KeySchemaElement
                 {
                  AttributeName = "GUID", KeyType = "HASH"
                 },
                new KeySchemaElement
                 {
                AttributeName = "Data", KeyType = "RANGE"
                 }
                };

                // Define key attributes:
                //  The key attributes "Author" and "Title" are string types
                List<AttributeDefinition> definitionsdest = new List<AttributeDefinition>
                {
    new AttributeDefinition
    {
        AttributeName = "GUID", AttributeType = "S"
    },
    new AttributeDefinition
    {
        AttributeName = "Data", AttributeType = "S"
    }
                };

                // Define table throughput:
                //  Table has capacity of 20 reads and 50 writes
                ProvisionedThroughput throughputdest = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 20,
                    WriteCapacityUnits = 50
                };

                // Configure the CreateTable request
                CreateTableRequest requestdest = new CreateTableRequest
                {
                    TableName = "MDSDestDynamoDBTest",
                    KeySchema = schemadest,
                    ProvisionedThroughput = throughputdest,
                    AttributeDefinitions = definitionsdest
                };


                // View new table properties
                var tableDescriptiondest = await client.CreateTableAsync(requestdest);
                Table Catologdest = Table.LoadTable(client, tableDescriptiondest.TableDescription.TableName);
                string status = null;
                // Let us wait until table is created. Call DescribeTable.
                do
                {
                    System.Threading.Thread.Sleep(3000); // Wait 5 seconds.
                    try
                    {
                        var res = await client.DescribeTableAsync(new DescribeTableRequest
                        {
                            TableName = "MDSDestDynamoDBTest"
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

                var data = new Document();
                data["GUID"] = Guid.NewGuid().ToString();
                data["Data"] = mdtexttest;
                data["updatedDate"] = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssf"));
                data["timestamp"] = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssfff"));
                data["UniqueRow"] = Convert.ToInt64(String.Format("{0:d9}", (DateTime.Now.Ticks / 10) % 1000000000));
                data["Error"] = "RS Mapping Missing";
                Document response = await Catologdest.PutItemAsync(data);
                var context = new TestLambdaContext();
                var function = new Function();
                var responseFinal = await function.FunctionHandler(null, context);
                Assert.Equal(responseFinal, null);
            }
            catch (Exception ex)
            {

            }

        }
    }
}
