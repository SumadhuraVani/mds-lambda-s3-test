using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace MDS_Lambda_S3
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }
        static int maxRetryAttempts = 3;
        static TimeSpan pauseBetweenFailures = TimeSpan.FromSeconds(2);        
        private static string tableName = "MDSSourceDynamoDBTestDat1";
        private static string RSMappedDestination = "KeyRatingCreditType";
        private static string urlData = "http://chomisvcdev.snl.int/SNL.Services.Data.Api.Service/v2/Internal/Editable/";
        private static string DomainName = "snl";
        private static string UserName = "imageservice";
        private static string Password = "R0sencrantz";
        private static string RSMappedSource = "CREDIT_TYPE";
        private static string KeyCurrency = "LookupCurrencys?$select=KeyCurrency&$filter=((UpdOperation+eq+0)or(UpdOperation+eq+1))&$skip=0&$top=250&editable=true";
        private static string RSMapTableValue = "10197";
        private static string RSMapQuery = "GMISNLCIQMappings?select=OID,OIDCIQ&$filter=(({0}) and KeyTable eq {1} and KeyMINameSpace eq 5)&editable=true";
        private static string DestinationS3 = "spgmi-dest-buck-test";
        private static string RSMappedQuery = "GMISNLCIQMappings?select=OID,OIDCIQ&$filter=(({0}) and KeyTable eq {1} and KeyMINameSpace eq 5)&editable=true";
        private static string CurrencyCheckColumn = "KeyCurrency";
        
        //private static string tableName = Environment.GetEnvironmentVariable("SourceDynamoDB");
        //private static string RSMappedDestination = Environment.GetEnvironmentVariable("RSMappedDestination");
        //private static string urlData = Environment.GetEnvironmentVariable("URL");
        //private static string DomainName = Environment.GetEnvironmentVariable("Domain");
        //private static string UserName = Environment.GetEnvironmentVariable("UserName");
        //private static string Password = Environment.GetEnvironmentVariable("Password");
        //private static string RSMappedSource = Environment.GetEnvironmentVariable("RSMappedSource");
        //private static string KeyCurrency = Environment.GetEnvironmentVariable("KeyCurrencyQuery");
        //private static string RSMapTableValue = Environment.GetEnvironmentVariable("RSMapTableValue");
        //private static string RSMapQuery = Environment.GetEnvironmentVariable("RSMappedQuery");
        //private static string DestinationS3 = Environment.GetEnvironmentVariable("DestinationS3");
        //private static string RSMappedQuery = Environment.GetEnvironmentVariable("RSMappedQuery");
        //private static string CurrencyCheckColumn = Environment.GetEnvironmentVariable("CurrencyCheckColumn");


        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.USEast1;
        private static object _locker = new object();
        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>

        private static async Task<string> CreateTableItem(Table Catalog, string jsonData, ILambdaContext context, string RSMappedData)
        {
            try
            {
                // context.Logger.Log("Vani Before Insertion" + RSMappedData);
                var data = new Document();
                data["guid"] = Guid.NewGuid().ToString();
                data["Data"] = jsonData;
                data["RSMappedValues"] = RSMappedData;
                data["timestamp"] = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssfff"));
                data["UniqueRow"] = Convert.ToInt64(String.Format("{0:d9}", (DateTime.Now.Ticks / 10) % 1000000000));
                data["Error"] = "RS Mapping Missing";
                Document response = await Catalog.PutItemAsync(data);
                context.Logger.Log("Data Inserted in dynamo DB" + jsonData);
                return "Success";
            }
            catch (Exception e)
            {
                context.Logger.Log("Error" + e.ToString());
                return "Failure";
            }
        }

        public static StringBuilder GetallRecordsWhereClause(List<string> Values)
        {
            StringBuilder wherequery = new StringBuilder();
            foreach (var value in Values)
            {
                if (wherequery.Length > 1)
                    wherequery.Append("or");
                wherequery.Append(string.Format("(OIDCIQ+eq+'{0}')", value));
            }

            return wherequery;
        }
        public class KeyCurrencys
        {
            public string KeyCurrency { get; set; }
        }
        public class RsMappings
        {
            public string OID { get; set; }
            public string OIDCIQ { get; set; }
            public DateTime PublishDate { get; set; }
        }

        public static JToken ExecuteODataGetQuery(string ODataQueryFilter, ILambdaContext context)
        {
            try
            {
                string ODataErrorResponse = string.Empty;
                AppContext.SetSwitch("System.Net.Http.UseSocketsHttpHandler", false);
                string URL = urlData + ODataQueryFilter;
                string responseStr = string.Empty;
                HttpWebRequest req = (HttpWebRequest)WebRequest.Create(URL);
                NetworkCredential credential = new NetworkCredential(UserName, Password, DomainName);
                CredentialCache credentialCache = new CredentialCache();
                credentialCache.Add(new Uri(URL), "NTLM", credential);
                req.Credentials = credentialCache;
                try
                {
                    RetryHelper.RetryOnException(maxRetryAttempts, pauseBetweenFailures, () =>
                    {
                        HttpWebResponse response = (HttpWebResponse)req.GetResponse();
                        using (Stream dataStream = response.GetResponseStream())
                        {
                            StreamReader reader = new StreamReader(dataStream);
                            responseStr = reader.ReadToEnd();
                        }
                    }, context);
                }
                catch (Exception ex)
                {
                    context.Logger.LogLine("Error" + ex.ToString());
                    return null;
                }
                context.Logger.Log(responseStr);
                var contentJo = (JObject)JsonConvert.DeserializeObject(responseStr);
                JObject jsonResponseRatingEntity = (JObject)Newtonsoft.Json.JsonConvert.DeserializeObject(responseStr,
                                                                new Newtonsoft.Json.JsonSerializerSettings()
                                                                {
                                                                    DateParseHandling = Newtonsoft.Json.DateParseHandling.None
                                                                });
                if (jsonResponseRatingEntity != null)
                    return jsonResponseRatingEntity["value"];
                else
                    return null;
            }
            catch (Exception ex)
            {
                context.Logger.Log("Error" + ex.ToString());
                return null;
            }

        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null)
            {
                context.Logger.LogLine("No S3 Events");
                return null;
            }
            try
            {
                //s3Client = new AmazonS3Client();
                // S3Client = new AmazonS3Client(bucketRegion);
                //Variable Declarations
             
                var response = await this.S3Client.GetObjectAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                string line = string.Empty;
                List<string> SourceRows = new List<string>();
                List<string> GetSourceItems = new List<string>();
                List<string> DestinationData = new List<string>();
                Dictionary<string, string> RSMappedItems = new Dictionary<string, string>();
                List<RsMappings> rsMappings = new List<RsMappings>();
                List<KeyCurrencys> keyCurrencies = new List<KeyCurrencys>();
                List<List<RsMappings>> rsData = new List<List<RsMappings>>();
                List<dynamic> dynmWithRS = new List<dynamic>();
                List<dynamic> dynmWithOutRS = new List<dynamic>();
                string Output = string.Empty;
                JObject jsonData = null;
                List<MyDynObject> dynm = new List<MyDynObject>();
                string RSMapped = RSMappedSource;
                string[] RSMappedSplit = RSMapped.Split(',').ToArray();//for sector
                string[] RSMappedDestinationSplit = RSMappedDestination.Split(',').ToArray();//for sector
                Dictionary<string, object> GetDictionaryItems = new Dictionary<string, object>();
                context.Logger.LogLine(s3Event.Bucket.Name + s3Event.Object.Key);
               
                DestinationData.Add("RatingDataAsOf");
                DestinationData.Add("KeyCurrency");
                DestinationData.Add("KeyRatingCreditType");
                DestinationData.Add("CreditRatingDerivedCDS");
                DestinationData.Add("CDSBenchmarkSpread");
                DestinationData.Add("CDSBenchmarkSpread1Day");
                DestinationData.Add("CDSBenchmarkSpread7Day");
                DestinationData.Add("CDSBenchmarkSpread30Day");
                DestinationData.Add("CDSBenchmarkSpread90Day");
                DestinationData.Add("CDSBenchmarkSpread1Year");
                DestinationData.Add("CDSBenchmarkSpread2Year");
                DestinationData.Add("CDSBenchmarkSpread3Year");
                DestinationData.Add("RatingPublishDate");

                //Read data from s3 and convert to Object
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    line = await reader.ReadToEndAsync();
                }

                context.Logger.LogLine("S3 Event Read Successful" + s3Event.Bucket.Name + s3Event.Object.Key);
                string[] lines = line.Split('\n').ToArray();

                foreach (string items in lines[0].Remove(0, 1).Split('|').ToList())
                {
                    GetSourceItems.Add(items);
                }
                lines = lines.Skip(1).ToArray();
                if (lines[lines.Count() - 1].Equals(""))
                    lines = lines.Take<string>(lines.Count() - 1).ToArray();
                if (lines[lines.Count() - 1].StartsWith("RECORD"))
                    lines = lines.Take<string>(lines.Count() - 1).ToArray();
                foreach (string dataline in lines)
                {
                    GetDictionaryItems = new Dictionary<string, object>();
                    SourceRows = dataline.Split('|').ToList();
                    for (int i = 0; i <= GetSourceItems.Count - 1; i++)
                    {
                        //if (GetSourceItems[i].ToString() == RSMapped && !RSMappedItems.Contains(SourceRows[i]))
                        if (RSMappedSplit.Contains(GetSourceItems[i].ToString()) && !RSMappedItems.ContainsKey(SourceRows[i]) && SourceRows[i].ToString() != "--")
                            RSMappedItems.Add(SourceRows[i], GetSourceItems[i]);
                        GetDictionaryItems.Add(GetSourceItems[i], SourceRows[i]);
                    }
                    MyDynObject dyn = DynamicSource.DynamicObject(GetDictionaryItems);
                    dynm.Add(dyn);
                }
                //check currency data

                string Currencyquery = string.Format(KeyCurrency);
                var ODataCurrencyRsponse = ExecuteODataGetQuery(Currencyquery, context);
                keyCurrencies.AddRange(ODataCurrencyRsponse.ToObject<List<KeyCurrencys>>());

                //Chcek RS Mapping              
                for (int i = 0; i < RSMappedSplit.Count(); i++)
                {
                    List<string> RSMappedDataAfterSplit = RSMappedItems.Where(x => x.Value.Equals(RSMappedSplit.ElementAt(i))).Select(y => y.Key).ToList();
                    var whereClause = GetallRecordsWhereClause(RSMappedDataAfterSplit);
                    string RSmapquery = string.Format(RSMappedQuery, whereClause, RSMapTableValue.Split(',')[i]);

                    var ODataRsponse1 = ExecuteODataGetQuery(RSmapquery, context);
                    if (ODataRsponse1 != null && ODataRsponse1.HasValues)
                        rsMappings.AddRange(ODataRsponse1.ToObject<List<RsMappings>>());
                    // rsData.Add(rsMappings);
                    context.Logger.LogLine("RS Mapping data fetch Successful");
                }

                string json = JsonConvert.SerializeObject(dynm);
                int numb = 0;
                foreach (string columnname in GetSourceItems)
                {
                    var envVariable = DestinationData.ElementAt(numb);
                    //var envVariable = Environment.GetEnvironmentVariable(columnname);//Need to Enable Again
                    if (json.Contains(columnname) && !string.IsNullOrEmpty(envVariable))
                        json = json.Replace(columnname, envVariable.ToString());
                    numb++;
                }

                var fileTransferUtility =
                   new TransferUtility(this.S3Client);
                dynamic dynamoClass = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json);

                //Segregate Data with RS Mapped and Without RS Mapped
                foreach (dynamic x in dynamoClass)
                {
                    int rsmapped = 0;
                    int rsunmapped = 0;
                    context.Logger.LogLine(x.ToString());
                    jsonData = JObject.Parse(x.ToString());

                    if (keyCurrencies != null && keyCurrencies.Select(map => map.KeyCurrency == jsonData.SelectToken(CurrencyCheckColumn).ToString()).Count() == 0)
                    {
                        continue;
                    }
                    foreach (string str in RSMappedDestinationSplit)
                    {
                        if (string.IsNullOrEmpty(jsonData.SelectToken(str).ToString()) || jsonData.SelectToken(str).ToString().Equals("--"))
                            continue;
                        var RSData = rsMappings != null ? rsMappings.FirstOrDefault(mapping => mapping.OIDCIQ == jsonData.SelectToken(str).ToString()) : null;
                        if (RSData != null)
                        {
                            x[str].Value = RSData.OID;
                            // dynmWithRS.Add(x);
                            rsmapped++;

                        }
                        else
                        {
                            //dynmWithOutRS.Add(x);
                            rsunmapped++;
                        }
                    }
                    if (rsmapped == RSMappedDestinationSplit.Count())
                        dynmWithRS.Add(x);
                    else if (rsunmapped > 0)
                        dynmWithOutRS.Add(x);
                }

                //Transfer RS Mapped data to destination s3 Bucket
                if (dynmWithRS.Count > 0)
                {
                    context.Logger.LogLine("Destination Data Move Count" + dynmWithRS.Count);
                    for (int i = 0; i < dynmWithRS.Count; i = i + 250)
                    {
                        List<dynamic> dynamoSkip = dynmWithRS.Skip(i).Take(250).ToList();
                        string RSMappedjson = JsonConvert.SerializeObject(dynamoSkip);
                        PutObjectRequest putRequest = null;
                        lock (_locker)
                        {
                            putRequest = new PutObjectRequest
                            {
                                BucketName = DestinationS3,//later need to move to env var
                                Key = "Json" + Guid.NewGuid().ToString(),
                                ContentBody = RSMappedjson
                            };
                            context.Logger.LogLine(putRequest.Key);
                        }
                        PutObjectResponse responseData = await this.S3Client.PutObjectAsync(putRequest);
                        context.Logger.LogLine("Data Successfully converted and moved to Destination Lambda");
                    }
                }

                //transfer RS Unmapped data to DynamoDB

                if (dynmWithOutRS.Count > 0)
                {
                    context.Logger.LogLine("DynamoDB Count" + dynmWithOutRS.Count.ToString());
                    AmazonDynamoDBClient client = new AmazonDynamoDBClient(bucketRegion);
                    Table Catalog = Table.LoadTable(client, tableName);

                    foreach (dynamic x in dynmWithOutRS)
                    {
                        jsonData = JObject.Parse(x.ToString());
                        string jsonRSMappData = string.Empty;
                        foreach (string str in RSMappedDestinationSplit)
                        {
                            jsonRSMappData = string.IsNullOrEmpty(jsonRSMappData) ? jsonData.SelectToken(str).ToString() : jsonRSMappData + "#" + jsonData.SelectToken(str).ToString();
                        }
                        var resData=await CreateTableItem(Catalog, x.ToString(), context, jsonRSMappData);
                    }
                    context.Logger.LogLine("RS Unmapped data moved to dynamoDB");
                }
                int milliseconds = 3000;
                Thread.Sleep(milliseconds);
                context.Logger.LogLine("Success");
                return "Success";

            }
            catch (Exception e)
            {
                //context.Logger.LogLine($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogLine(e.ToString());
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine("Error");
                return "Error";
            }
        }
    }
}
