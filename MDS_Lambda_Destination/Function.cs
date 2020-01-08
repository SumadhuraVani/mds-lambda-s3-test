using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using System.Text;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Newtonsoft.Json.Linq;
using System.Globalization;
using System.Net.Http;
using System.Net;
using Newtonsoft.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace MDS_Lambda_Destination
{
    public class Function
    {
        //Test Env
        //static string ProxyHost = Environment.GetEnvironmentVariable("ProxyHost");
        //static int ProxyPort = Convert.ToInt32(Environment.GetEnvironmentVariable("ProxyPort"));
        private static string DestinationQuery = "KeyRatingCreditType:int,CreditRatingDerivedCDS:string,KeyCurrency:string,RatingDataAsOf:DateTime";
        private static string DestinationTableData = "KeyRatingCreditType:string,CreditRatingDerivedCDS:string,KeyCurrency:string,RatingDataAsOf:DateTime,CDSBenchmarkSpread:Double,CDSBenchmarkSpread1Day:Double,CDSBenchmarkSpread7Day:Double,CDSBenchmarkSpread30Day:Double,CDSBenchmarkSpread90Day:Double,CDSBenchmarkSpread1Year:Double,CDSBenchmarkSpread2Year:Double,CDSBenchmarkSpread3Year:Double,RatingPublishDate:DateTime";
        private static string Domain = "snl";
        private static string EntityKey = "KeyRatingBenchmarkCDS";
        private static string EntityName = "RatingBenchmarkCDSs";
        private static string EnvironmentVar = "(KeyRatingCreditType eq {0} and CreditRatingDerivedCDS eq {1} and KeyCurrency eq {2} and RatingDataAsOf eq {3} )";
        private static string Password = "R0sencrantz";

        private static string RSMappedDestination = "KeyRatingCreditType";
        private static string SecretKeyName = "SNLSQLDevODataCreds";
        private static string SourceDynamoDB = "MDSSourceDynamoDB";
        private static string URL = "http://chomisvcdev.snl.int/SNL.Services.Data.Api.Service/v2/Internal/Editable/";
        private static string URL_Data = "http://chomisvcdev.snl.int/SNL.Services.Data.Api.Service/v2/Internal/Editable/RatingBenchmarkCDSs";
         private static string UserName = "imageservice";
        private static string envrVariable = "RatingBenchmarkCDSs?select=KeyRatingBenchmarkCDS,KeyRatingCreditType,CreditRatingDerivedCDS,KeyCurrency,RatingDataAsOf&$filter=(";

        private static string tableName = "MDSSourceDynamoDBTest1";
        

        IAmazonS3 S3Client { get; set; }
        static int maxRetryAttempts = 3;
        static TimeSpan pauseBetweenFailures = TimeSpan.FromSeconds(2);
        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.USEast1;
        //static string ProxyHost = Environment.GetEnvironmentVariable("ProxyHost");
        //static int ProxyPort = Convert.ToInt32(Environment.GetEnvironmentVariable("ProxyPort"));
        //private static AmazonDynamoDBConfig config = new AmazonDynamoDBConfig()
        //{
        //    RegionEndpoint = bucketRegion,
        //    ProxyHost = ProxyHost,
        //    ProxyPort = ProxyPort
        //};
        
        //private static string DestinationQuery = Environment.GetEnvironmentVariable("DestinationQuery");
        //private static string DestinationTableData = Environment.GetEnvironmentVariable("DestinationTableData");
        //private static string Domain = Environment.GetEnvironmentVariable("Domain");
        //private static string EntityKey = Environment.GetEnvironmentVariable("EntityKey");
        //private static string EntityName = Environment.GetEnvironmentVariable("EntityName");
        //private static string EnvironmentVar = Environment.GetEnvironmentVariable("EnvironmentVar");
        //private static string Password = Environment.GetEnvironmentVariable("Password");

        //private static string RSMappedDestination = Environment.GetEnvironmentVariable("RSMappedDestination");
        //private static string SecretKeyName = Environment.GetEnvironmentVariable("SecretKeyName");
        //private static string SourceDynamoDB = Environment.GetEnvironmentVariable("SourceDynamoDB");
        //private static string URL = Environment.GetEnvironmentVariable("URL");
        //private static string URL_Data = Environment.GetEnvironmentVariable("URL_Data");
       // private static string UserName = Environment.GetEnvironmentVariable("UserName");
        //private static string envrVariable = Environment.GetEnvironmentVariable("envrVariable");

        //private static string tableName = Environment.GetEnvironmentVariable("SourceDynamoDB");
        private static IAmazonS3 s3Client;
        Table Catalog = null;
       // public static string UserName = string.Empty;
        //public static string Password = string.Empty;
        // private static AmazonSQSClient sqsClient;


       
        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            s3Client = new AmazonS3Client();
            AmazonDynamoDBConfig config = new AmazonDynamoDBConfig();
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(config);
            try
            {
                RetryHelper.RetryOnException(maxRetryAttempts, pauseBetweenFailures, () =>
                {
                    Catalog = Table.LoadTable(client, tableName);
                }, null);
            }
            catch (Exception ex)
            {
            }
            // sqsClient = new AmazonSQSClient(bucketRegion);
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }
        public async Task<string> GetSecret(ILambdaContext context)
        {
            try
            {
                string secretName = "SecretKeyName";
                string secret = string.Empty;
                MemoryStream memoryStream = new MemoryStream();
                //var config = new AmazonSecretsManagerConfig
                //{
                //    RegionEndpoint = bucketRegion,
                //    ProxyHost = ProxyHost,
                //    ProxyPort = ProxyPort
                //};
                var config = new AmazonSecretsManagerConfig
                {
                    RegionEndpoint = bucketRegion,
                    //ProxyHost = ProxyHost,
                    //ProxyPort = ProxyPort
                };
                var client1 = new AmazonSecretsManagerClient(config);
                var request = new GetSecretValueRequest
                {
                    SecretId = secretName,
                    VersionStage = "AWSCURRENT"
                };
                GetSecretValueResponse response = null;
                try
                {
                    response = Task.Run(async () => await client1.GetSecretValueAsync(request)).Result;
                }
                catch (DecryptionFailureException ex)
                {
                    context.Logger.Log(ex.ToString());
                }
                catch (InternalServiceErrorException ex)
                {

                    context.Logger.Log(ex.ToString());
                }
                catch (InvalidParameterException e)
                {

                    context.Logger.Log(e.ToString());
                }
                catch (InvalidRequestException ex)
                {

                    context.Logger.Log(ex.ToString());
                }
                catch (System.AggregateException ex)
                {

                    context.Logger.Log(ex.ToString());
                }

                catch (Exception ex)
                {
                    context.Logger.Log("Secret Key Failure exception");
                    context.Logger.Log(ex.ToString());
                }
                context.Logger.Log(response.SecretString);
                if (response.SecretString != null)
                {
                    secret = response.SecretString;
                }
                else
                {
                    memoryStream = response.SecretBinary;
                    StreamReader reader = new StreamReader(memoryStream);
                    secret = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(reader.ReadToEnd()));
                }
                return secret;
            }
            catch(Exception ex)
            {
                return null;
            }
        }
        private static async Task<string> CreateTableItem(Table Catalog, string jsonData, ILambdaContext context, string RSMappedData, string Error)
        {
            try
            {
                // context.Logger.Log("Vani Before Insertion" + RSMappedData);
                var data = new Document();
                data["guid"] = Guid.NewGuid().ToString();
                data["Data"] = jsonData;
                data["RSMappedValues"] = RSMappedData;
                data["Error"] = Error;
                data["timestamp"] = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssfff"));
                data["UniqueRow"] = Convert.ToInt64(String.Format("{0:d9}", (DateTime.Now.Ticks / 10) % 1000000000));
                Document response = await Catalog.PutItemAsync(data);
                context.Logger.Log("Data failed and inserted in Dynamo DB" + jsonData);
                return "Success";
            }
            catch (Exception e)
            {
                context.Logger.Log("Error" + e.ToString());
                return "Failure";
            }
        }

        public JToken ExecuteODataGetQuery(string ODataQueryFilter, ILambdaContext context)
        {
            try
            {
                string ODataErrorResponse = string.Empty;

                AppContext.SetSwitch("System.Net.Http.UseSocketsHttpHandler", false);
                string input = URL;
                string URLData = input + ODataQueryFilter;
                string responseStr = string.Empty;
                HttpWebRequest req = (HttpWebRequest)WebRequest.Create(URLData);
                NetworkCredential credential = new NetworkCredential(UserName, Password, Domain);
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
                    context.Logger.LogLine(ex.ToString());
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
                context.Logger.Log(ex.ToString());
                return null;
            }

        }
        public enum DBAction
        {
            None,
            INSERT,
            UPDATE,
            DELETE
        }

        public DBAction GetODataAction(string dbAction)
        {
            switch (dbAction)
            {
                case "Insert":
                    return DBAction.INSERT;
                case "Update":
                    return DBAction.UPDATE;
                default:
                    throw new Exception($"{dbAction} DB Operation does not supported");
            }
        }

        public Dictionary<string, object> GenerateODataObject(ILambdaContext context, dynamic dbEntity, string MItargetObject, Dictionary<string, string> DestinationTypesdata, string action, Dictionary<string, string> TypesData)
        {
            try
            {

                //context.Logger.Log(action);
                //string NullableData = Environment.GetEnvironmentVariable("NullableData");
                //string[] ColValues = NullableData.Split(',').ToArray();
                JObject json = JObject.Parse(dbEntity.ToString());
                DateTime dDate;
                Dictionary<string, object> EntityFields = new Dictionary<string, object>();

                if (action.ToLower() == DBAction.INSERT.ToString().ToLower())
                    EntityFields.Add(EntityKey, Guid.NewGuid().ToString());

                //int i = 0;
                foreach (KeyValuePair<string, string> ele in DestinationTypesdata)
                {

                    //i++;
                    if (action.ToLower() == DBAction.UPDATE.ToString().ToLower())
                    {
                        if (TypesData.ContainsKey(ele.Key))
                            continue;
                    }

                    switch (ele.Value)
                    {
                        case "Double":
                            if (!string.IsNullOrEmpty(json.SelectToken(ele.Key).ToString()))
                            {
                                EntityFields.Add(ele.Key, Convert.ToDouble(json.SelectToken(ele.Key).ToString()));
                            }
                            //else if (Array.Find(ColValues, elem => elem.Contains(ele.Key + ":No")) != null && Array.Find(ColValues, elem => elem.Contains(ele.Key + ":No")).Count() >= 0)
                            // EntityFields.Add(ele.Key, 0);
                            else
                                EntityFields.Add(ele.Key, null);
                            break;
                        case "DateTime":
                            if (DateTime.TryParseExact(json.SelectToken(ele.Key).ToString(), "yyyyMMdd",
                     CultureInfo.InvariantCulture, DateTimeStyles.None, out dDate))
                            {
                                EntityFields.Add(ele.Key, dDate.ToString("yyyy-MM-dd"));
                            }
                            else
                            {
                                if (string.IsNullOrEmpty(json.SelectToken(ele.Key).ToString()))
                                    EntityFields.Add(ele.Key, null);
                                else
                                    EntityFields.Add(ele.Key, Convert.ToDateTime(json.SelectToken(ele.Key).ToString()));
                            }
                            break;
                        case "string":
                            EntityFields.Add(ele.Key, json.SelectToken(ele.Key).ToString());
                            break;
                        case "int":
                            EntityFields.Add(ele.Key, Convert.ToInt16(json.SelectToken(ele.Key).ToString()));
                            break;
                    }

                }

                return EntityFields;
            }
            catch (Exception ex)
            {
                context.Logger.Log(ex.ToString());
                return null;
            }
        }
        public async Task<string> InsertOdateResponseAsync(string jsonData, ILambdaContext context, dynamic dbEntity)
        {
            try
            {
                context.Logger.Log(jsonData);
                AppContext.SetSwitch("System.Net.Http.UseSocketsHttpHandler", false);
                var httpWebRequest = (HttpWebRequest)WebRequest.Create(URL_Data);
                httpWebRequest.ContentType = "application/json";
                httpWebRequest.Method = "POST";
                NetworkCredential credential = new NetworkCredential(UserName, Password, Domain);
                CredentialCache credentialCache = new CredentialCache();
                credentialCache.Add(new Uri(URL_Data), "NTLM", credential);
                httpWebRequest.Credentials = credentialCache;
                using (StreamWriter streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                {
                    streamWriter.Write(jsonData);
                    streamWriter.Flush();
                    streamWriter.Close();
                }

                RetryHelper.RetryOnException(maxRetryAttempts, pauseBetweenFailures, () => {
                    HttpWebResponse httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                    context.Logger.Log(httpResponse.StatusCode.ToString());
                }, context);


                return "Success";
            }
            catch (Exception ex)
            {
                AmazonDynamoDBConfig config = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient(config);
                context.Logger.LogLine(jsonData);
                JObject jsonObj = JObject.Parse(dbEntity.ToString());
                if (Catalog == null)
                    RetryHelper.RetryOnException(maxRetryAttempts, pauseBetweenFailures, () =>
                    {
                        Catalog = Table.LoadTable(client, tableName);
                    }, context);
                string Response = await CreateTableItem(Catalog, dbEntity.ToString(), context, jsonObj.SelectToken(RSMappedDestination).ToString(), ex.ToString());
                context.Logger.Log("msg sent to DB" + Response);
                context.Logger.LogLine(ex.ToString());
                return "Error";
            }
        }


        public async Task<string> UpdateOdateResponseAsync(string jsonData, ILambdaContext context, string key, dynamic dbEntity)
        {
            try
            {

                string input = "(" + key + ")";
                AppContext.SetSwitch("System.Net.Http.UseSocketsHttpHandler", false);
                var httpWebRequest = (HttpWebRequest)WebRequest.Create(URL_Data + input);
                httpWebRequest.ContentType = "application/json";
                httpWebRequest.Method = "PATCH";
                NetworkCredential credential = new NetworkCredential(UserName, Password, Domain);
                CredentialCache credentialCache = new CredentialCache();
                credentialCache.Add(new Uri(URL_Data), "NTLM", credential);
                httpWebRequest.Credentials = credentialCache;
                using (StreamWriter streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                {

                    streamWriter.Write(jsonData);
                    streamWriter.Flush();
                    streamWriter.Close();
                }
                context.Logger.Log(jsonData);
                RetryHelper.RetryOnException(maxRetryAttempts, pauseBetweenFailures, () => {
                    HttpWebResponse httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                    context.Logger.Log(httpResponse.StatusCode.ToString());

                }, context);
                return "Success";
            }
            catch (Exception ex)
            {
                AmazonDynamoDBConfig config = new AmazonDynamoDBConfig();
                AmazonDynamoDBClient client = new AmazonDynamoDBClient(config);
                JObject jsonObj = JObject.Parse(dbEntity.ToString());
                if (Catalog == null)
                    RetryHelper.RetryOnException(maxRetryAttempts, pauseBetweenFailures, () =>
                    {
                        Catalog = Table.LoadTable(client, tableName);
                    }, context);
                string Response = await CreateTableItem(Catalog, dbEntity.ToString(), context, jsonObj.SelectToken(RSMappedDestination).ToString(), ex.ToString());
                context.Logger.Log("msg sent to DB" + Response);
                context.Logger.LogLine(ex.ToString());
                return "Error";
            }
        }


        public async Task<string> ProcessData(string line,ILambdaContext context)
        {
            try
            {
                string[] ColValues = DestinationQuery.Split(',').ToArray();
                List<string> ColumnDestination = new List<string>();
                List<JArray> jArray = new List<JArray>();
                int j = 0, count = 0;
                string OldValue;
                string ResponseData = string.Empty;
                string jsonSerialize = string.Empty;
                string KeyValue = string.Empty;
                string action = "Insert";
                List<dynamic> ChunkData = new List<dynamic>();

                DateTime ParsedDate = new DateTime();
                StringBuilder wherequery = new StringBuilder();
                dynamic myclass = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(line);
                int loops = myclass.Count;
                context.Logger.LogLine(loops.ToString());
                List<string> resValues = new List<string>();                
                Dictionary<string, string> Typesdata = new Dictionary<string, string>();
                foreach (string str in ColValues)
                {
                    Typesdata.Add(str.Split(':')[0], str.Split(':')[1]);
                }

                string[] DestinationColumns = DestinationTableData.Split(',').ToArray();
                //string envrVariable = "RatingBenchmarkCDSs?select=KeyRatingBenchmarkCDS,KeyRatingCreditType,CreditRatingDerivedCDS,KeyCurrency,RatingPublishDate&$filter=(";
                Dictionary<string, string> DestinationTypesdata = new Dictionary<string, string>();
                string EnvironmentVarChild = string.Empty;
                foreach (string str in DestinationColumns)
                {
                    DestinationTypesdata.Add(str.Split(':')[0], str.Split(':')[1]);
                }

                context.Logger.LogLine("Json Data read succesful");

                foreach (var value in myclass)
                {

                    j++;
                    JObject json = JObject.Parse(value.ToString());
                    if (j > 25)
                    {
                        loops = loops - 25;
                        wherequery = new StringBuilder();
                        j = 1;
                    }

                    EnvironmentVarChild = EnvironmentVar;
                    resValues = new List<string>();

                    foreach (var x in Typesdata)
                    {
                        if (string.IsNullOrEmpty(json.SelectToken(x.Key).ToString()))
                            break;
                        if (x.Value.Equals("DateTime"))
                        {
                            try
                            {
                                ParsedDate = DateTime.ParseExact(json.SelectToken(x.Key).ToString(), "yyyyMMdd",
                               CultureInfo.InvariantCulture);
                                resValues.Add(ParsedDate.ToString("yyyy-MM-dd"));
                            }
                            catch (Exception ex)
                            {
                                context.Logger.Log(ex.ToString());
                                break;
                            }
                        }
                        else if (x.Value.Equals("int") || x.Value.Equals("Float"))
                            resValues.Add(json.SelectToken(x.Key).ToString());
                        else
                            resValues.Add("'" + json.SelectToken(x.Key).ToString() + "'");
                    }
                    if (!resValues.Count.Equals(Typesdata.Count))
                    {
                        context.Logger.Log("Invalid Data ignored.Please provide valid data" + value.ToString());
                        //continue;
                    }
                    else
                    {
                        ChunkData.Add(value);
                        if (wherequery.Length > 1)
                            wherequery.Append("or");
                        int i = 0;
                        foreach (string x in resValues)
                        {
                            OldValue = "{" + i + "}";
                            EnvironmentVarChild = EnvironmentVarChild.Replace(OldValue, x);
                            i++;
                        }
                        wherequery.Append(EnvironmentVarChild);
                    }
                    if (loops == j || j % 25 == 0)
                    {
                        //Read data from snlDB
                        context.Logger.Log("data entered");
                        var queryToCheckMDSCDSInDB = string.Format(envrVariable + wherequery + ")&editable=true");
                        queryToCheckMDSCDSInDB = queryToCheckMDSCDSInDB.Replace("+", "%2B");
                        var ODataRsponse = ExecuteODataGetQuery(queryToCheckMDSCDSInDB, context);
                        JArray ResponseArray = ODataRsponse != null ? JArray.Parse(ODataRsponse.ToString()) : null;

                        foreach (var obj in ChunkData)
                        {

                            JObject jsonData = JObject.Parse(obj.ToString());
                            action = "Insert";
                            if (ResponseArray != null)
                            {
                                foreach (JObject jobject in ResponseArray)
                                {
                                    count = 0;
                                    foreach (KeyValuePair<string, string> x in Typesdata)
                                    {
                                        switch (x.Value)
                                        {
                                            case "Double":
                                            case "string":
                                            case "int":
                                                if (jsonData.SelectToken(x.Key).ToString().Equals(jobject.SelectToken(x.Key).ToString()))
                                                    count++;
                                                break;
                                            case "DateTime":
                                                if (Convert.ToDateTime(jobject.SelectToken(x.Key).ToString()).ToString("yyyyMMdd").Equals(jsonData.SelectToken(x.Key).ToString()))
                                                    count++;
                                                break;
                                        }
                                    }
                                    if (Typesdata.Count == count)
                                    {
                                        action = "Update";
                                        KeyValue = jobject.SelectToken(EntityKey).ToString();
                                        break;
                                    }
                                    else
                                        action = "Insert";
                                }
                            }

                            //context.Logger.LogLine(obj.ToString());//enable afterwards
                            Dictionary<string, object> EntityFields = GenerateODataObject(context, obj, KeyValue, DestinationTypesdata, action, Typesdata);
                            if (EntityFields != null)
                            {
                                if (action == "Update")
                                {
                                    jsonSerialize = JsonConvert.SerializeObject(EntityFields);
                                    ResponseData = await UpdateOdateResponseAsync(jsonSerialize, context, KeyValue, obj);
                                }
                                else
                                {
                                    jsonSerialize = JsonConvert.SerializeObject(EntityFields);
                                    ResponseData = await InsertOdateResponseAsync(jsonSerialize, context, obj);
                                }
                            }
                            context.Logger.Log(ResponseData);
                        }

                        ChunkData = new List<dynamic>();
                    }

                }
                context.Logger.Log("Data successfully moved to DB");
                return "Success";
            }
            catch(Exception ex)
            {
                context.Logger.Log(ex.Message);
                return "Failure";
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
            //S3Client = new AmazonS3Client(bucketRegion);
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null)
            {
                return null;
            }
            try
            {
                //Variable Declarations
                string secretData = await GetSecret(context);
                if (!string.IsNullOrEmpty(secretData))
                {
                    context.Logger.LogLine(secretData);
                    JObject joBj = JObject.Parse(secretData);
                    UserName = joBj.SelectToken(UserName).ToString();
                    Password = joBj.SelectToken(Password).ToString();
                    context.Logger.LogLine(Password);
                }

                string line = string.Empty;
                // string ColValuesNullable = NullableData.Split(',').ToArray();
                context.Logger.LogLine(s3Event.Bucket.Name + s3Event.Object.Key);
                var response = await this.S3Client.GetObjectAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    line = await reader.ReadToEndAsync();
                }
                ProcessData(line, context);


                return "Success";
            }
            catch (Exception e)
            {
                context.Logger.LogLine("Error" + e.ToString());
                //context.Logger.LogLine($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                return "Error";
            }
        }
    }
}
