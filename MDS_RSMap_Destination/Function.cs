using System;
using System.IO;
using System.Text;

using Newtonsoft.Json;

using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Newtonsoft.Json.Linq;
using System.Globalization;
using System.Net.Http;
using System.Net;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Amazon;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace MDS_RSMap_Destination
{
    public class Function
    {
        //private static readonly JsonSerializer _jsonSerializer = new JsonSerializer();
        //static string ProxyHost = Environment.GetEnvironmentVariable("ProxyHost");
        //private static readonly RegionEndpoint bucketRegion = RegionEndpoint.USEast1;
        //static int ProxyPort = Convert.ToInt32(Environment.GetEnvironmentVariable("ProxyPort"));
        //private static AmazonDynamoDBConfig config = new AmazonDynamoDBConfig()
        //{
        //    RegionEndpoint = bucketRegion,
        //    ProxyHost = ProxyHost,
        //    ProxyPort = ProxyPort
        //};
        //private static AmazonDynamoDBClient client = new AmazonDynamoDBClient(config);
        //private static string tableName = Environment.GetEnvironmentVariable("DestinationDynamoDB");
        //static int maxRetryAttempts = 3;
        //static TimeSpan pauseBetweenFailures = TimeSpan.FromSeconds(2);
        //public static string UserName = string.Empty;
        //public static string Password = string.Empty;
        //public static string DestinationQuery = Environment.GetEnvironmentVariable("DestinationQuery");
        //public static string DestinationTableData = Environment.GetEnvironmentVariable("DestinationTableData");
        //public static string Domain = Environment.GetEnvironmentVariable("Domain");
        //public static string EnvironmentVar = Environment.GetEnvironmentVariable("EnvironmentVar");
        //public static string EntityName = Environment.GetEnvironmentVariable("EntityName");
        //public static string EntityKey = Environment.GetEnvironmentVariable("EntityKey");
        //public static string NullableData = Environment.GetEnvironmentVariable("NullableData");
        //public static string OdataURI = Environment.GetEnvironmentVariable("OdataURI");
        //public static string URLStr = Environment.GetEnvironmentVariable("URL");
        //public static string URL_Data = Environment.GetEnvironmentVariable("URL_Data");
        //public static string envrVariable = Environment.GetEnvironmentVariable("envrVariable");
        //public static string SecretKeyName= = Environment.GetEnvironmentVariable("SecretKeyName");
        //private static string tableName = Environment.GetEnvironmentVariable("DestinationDynamoDB");
        private static readonly JsonSerializer _jsonSerializer = new JsonSerializer();
        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.USEast1;
        private static AmazonDynamoDBConfig config = new AmazonDynamoDBConfig()
        {
            RegionEndpoint = bucketRegion
        };
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient(config);
        private static string tableName = "MDSDestDynamoDBTest";
        static int maxRetryAttempts = 3;
        static TimeSpan pauseBetweenFailures = TimeSpan.FromSeconds(2);
        public static string UserName = "imageservice";
        public static string Password = "R0sencrantz";
        public static string DestinationQuery= "KeyRatingCreditType:int,CreditRatingDerivedCDS:string,KeyCurrency:string,RatingDataAsOf:DateTime";
        public static string DestinationTableData = "KeyRatingCreditType:string,CreditRatingDerivedCDS:string,KeyCurrency:string,RatingDataAsOf:DateTime,CDSBenchmarkSpread:Double,CDSBenchmarkSpread1Day:Double,CDSBenchmarkSpread7Day:Double,CDSBenchmarkSpread30Day:Double,CDSBenchmarkSpread90Day:Double,CDSBenchmarkSpread1Year:Double,CDSBenchmarkSpread2Year:Double,CDSBenchmarkSpread3Year:Double,RatingPublishDate:DateTime";
        public static string Domain = "snl";
        public static string EnvironmentVar = "(KeyRatingCreditType eq {0} and CreditRatingDerivedCDS eq {1} and KeyCurrency eq {2} and RatingDataAsOf eq {3} )";
        public static string EntityName = "RatingBenchmarkCDSs";
        public static string EntityKey = "KeyRatingBenchmarkCDS";
        public static string NullableData = "CDSBenchmarkSpread:No";
        public static string OdataURI = "http://chomisvcdev.snl.int/SNL.Services.Data.Api.Service/v2/Internal/Editable/";
        public static string URLStr = "http://chomisvcdev.snl.int/SNL.Services.Data.Api.Service/v2/Internal/Editable/";
        public static string URL_Data = "http://chomisvcdev.snl.int/SNL.Services.Data.Api.Service/v2/Internal/Editable/RatingBenchmarkCDSs"; 
        public static string envrVariable = "RatingBenchmarkCDSs?select=KeyRatingBenchmarkCDS,KeyRatingCreditType,CreditRatingDerivedCDS,KeyCurrency,RatingDataAsOf&$filter=(";
        public static string SecretKeyName = "SNLSQLDevODataCreds";
        public async Task<string> GetSecret(ILambdaContext context)
        {
            
            string secretName = SecretKeyName;
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
                RegionEndpoint = bucketRegion
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
            
            catch (Exception ex)
            {
                context.Logger.Log("Secret Key Failure exception");
                context.Logger.Log(ex.ToString());
                return null;
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
        public async Task<string> FunctionHandler(DynamoDBEvent dynamoEvent, ILambdaContext context)
        {
            try
            {
                ScanFilter scan = new ScanFilter();
                DateTime ParsedDate = new DateTime();
                Table Catolog = null;
                //string NullableData = Environment.GetEnvironmentVariable("NullableData");
                RetryHelper.RetryOnException(maxRetryAttempts, pauseBetweenFailures, () =>
                {
                    Catolog = Table.LoadTable(client, tableName);
                }, context);
                scan.AddCondition("GUID", ScanOperator.IsNotNull);
                Search search = Catolog.Scan(scan);
                //////////
                //context.Logger.LogLine("1");
                List<string> ColumnDestination = new List<string>();
                List<JArray> jArray = new List<JArray>();
                int j = 0;

                int count = 0;
                string OldValue;
                string ResponseData = string.Empty;
                string jsonSerialize = string.Empty;
                string KeyValue = string.Empty;
                string action = "Insert";
                List<Tuple<string, string, string, string>> DestinationTuple = new List<Tuple<string, string, string, string>>();
                Dictionary<string, dynamic> ChunkData = new Dictionary<string, dynamic>();
                StringBuilder wherequery = new StringBuilder();
                List<string> resValues = new List<string>();
                // context.Logger.LogLine("2");
                string[] ColValues = DestinationQuery.Split(',').ToArray();
                Dictionary<string, string> Typesdata = new Dictionary<string, string>();
                foreach (string str in ColValues)
                {
                    Typesdata.Add(str.Split(':')[0], str.Split(':')[1]);
                }

                string[] DestinationColumns = DestinationTableData.Split(',').ToArray();
                Dictionary<string, string> DestinationTypesdata = new Dictionary<string, string>();
                string EnvironmentVarChild = string.Empty;
                foreach (string str in DestinationColumns)
                {
                    DestinationTypesdata.Add(str.Split(':')[0], str.Split(':')[1]);
                }
                Dictionary<string, DateTime> DynamoDBDate = new Dictionary<string, DateTime>();

                List<Document> documentList = new List<Document>();
                do
                {
                    documentList = await search.GetNextSetAsync();
                    foreach (Document doc in documentList)
                    {
                        DestinationTuple.Add(new Tuple<string, string, string, string>(doc["UniqueRow"].ToString(), doc["Data"].ToString(), doc["GUID"].ToString(), doc["timestamp"].ToString()));

                    }
                } while (!search.IsDone);

                int loops = DestinationTuple.Count;
                if (loops > 0)
                {
                    string secretData = await GetSecret(context);
                    if (!string.IsNullOrEmpty(secretData))
                    {
                        context.Logger.LogLine(secretData);
                        JObject joBj = JObject.Parse(secretData);
                        UserName = joBj.SelectToken(UserName).ToString();
                        Password = joBj.SelectToken(Password).ToString();
                        context.Logger.LogLine(Password);
                    }

                }
                context.Logger.LogLine(loops.ToString());
                string value = string.Empty;
                DestinationTuple = DestinationTuple.OrderBy(x => x.Item4).ToList();
                foreach (var kvp in DestinationTuple)
                {

                    context.Logger.LogLine(kvp.Item4);
                    j++;
                    dynamic myclass = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(kvp.Item2);
                    JObject json = JObject.Parse(myclass.ToString());

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
                        if (string.IsNullOrEmpty(json.SelectToken(x.Key).ToString()) || json.SelectToken(x.Key).ToString().Equals("--"))
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

                    }
                    else
                    {

                        ChunkData.Add(kvp.Item3, myclass);

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
                        var queryToCheckMDSCDSInDB = string.Format(envrVariable + wherequery + ")&editable=true");
                        queryToCheckMDSCDSInDB = queryToCheckMDSCDSInDB.Replace("+", "%2B");
                        var ODataRsponse = ExecuteODataGetQuery(queryToCheckMDSCDSInDB, context);
                        JArray ResponseArray = ODataRsponse != null ? JArray.Parse(ODataRsponse.ToString()) : null;
                        //jArray.Add(ResponseArray);
                        foreach (KeyValuePair<string, dynamic> obj in ChunkData)
                        {
                            JObject jsonData = JObject.Parse(obj.Value.ToString());
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
                            context.Logger.LogLine(obj.Value.ToString());
                            Dictionary<string, object> EntityFields = GenerateODataObject(context, obj.Value, KeyValue, DestinationTypesdata, action, Typesdata);
                            if (EntityFields != null)
                            {
                                context.Logger.LogLine(EntityFields.ElementAt(0).Value.ToString());
                                if (action == "Update")
                                {
                                    jsonSerialize = JsonConvert.SerializeObject(EntityFields);
                                    ResponseData = UpdateOdateResponse(jsonSerialize, context, KeyValue);
                                }
                                else
                                {
                                    jsonSerialize = JsonConvert.SerializeObject(EntityFields);
                                    ResponseData = InsertOdateResponse(jsonSerialize, context);
                                }

                                if (ResponseData == "Success")
                                {
                                    //Delete data from Dynamo DB
                                    DeleteItemRequest request = new DeleteItemRequest();
                                    request.TableName = tableName;
                                    request.Key = new Dictionary<string, AttributeValue>() { { "GUID", new AttributeValue { S = obj.Key } } };
                                    var response = await client.DeleteItemAsync(request);
                                }
                            }
                            context.Logger.Log(ResponseData);
                        }
                        ChunkData = new Dictionary<string, dynamic>();
                    }
                }
                context.Logger.Log("Success");
                return "Success";
            }
            catch (Exception ex)
            {
                context.Logger.Log("Error" + ex.ToString());
                return null;
            }
        }
        public JToken ExecuteODataGetQuery(string ODataQueryFilter, ILambdaContext context)
        {
            try
            {
                string ODataErrorResponse = string.Empty;
                AppContext.SetSwitch("System.Net.Http.UseSocketsHttpHandler", false);
                string input = URLStr;
                string URL = input + ODataQueryFilter;
                string responseStr = string.Empty;
                context.Logger.Log(URL);
                HttpWebRequest req = (HttpWebRequest)WebRequest.Create(URL);
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
                return jsonResponseRatingEntity["value"];
            }
            catch (Exception ex)
            {
                context.Logger.Log("Error" + ex.ToString());
                return "Error";
            }

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

                context.Logger.Log(action);
                // string NullableData = Environment.GetEnvironmentVariable("NullableData");
                //string[] ColValues = NullableData.Split(',').ToArray();
                JObject json = JObject.Parse(dbEntity.ToString());
                DateTime dDate;
                Dictionary<string, object> EntityFields = new Dictionary<string, object>();

                if (action.ToLower() == DBAction.INSERT.ToString().ToLower())
                    EntityFields.Add(EntityKey, Guid.NewGuid().ToString());

                int i = 0;
                foreach (KeyValuePair<string, string> ele in DestinationTypesdata)
                {

                    i++;
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
                            //  EntityFields.Add(ele.Key, 0);
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
        public string InsertOdateResponse(string jsonData, ILambdaContext context)
        {
            try
            {
                AppContext.SetSwitch("System.Net.Http.UseSocketsHttpHandler", false);
                var httpWebRequest = (HttpWebRequest)WebRequest.Create(URL_Data);
                httpWebRequest.ContentType = "application/json";
                httpWebRequest.Method = "POST";
                NetworkCredential credential = new NetworkCredential(UserName, Password, Domain);
                CredentialCache credentialCache = new CredentialCache();
                credentialCache.Add(new Uri(URL_Data), "NTLM", credential);
                httpWebRequest.Credentials = credentialCache;
                using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                {
                    streamWriter.Write(jsonData);
                    streamWriter.Flush();
                    streamWriter.Close();
                }
                context.Logger.LogLine(jsonData);
                RetryHelper.RetryOnException(maxRetryAttempts, pauseBetweenFailures, () => {
                    HttpWebResponse httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                    context.Logger.Log(httpResponse.StatusCode.ToString());
                }, context);
                return "Success";
            }
            catch (Exception ex)
            {
                context.Logger.LogLine(ex.ToString());
                return "Error";
            }
        }
        public enum DBAction
        {
            None,
            INSERT,
            UPDATE,
            DELETE
        }
        public string UpdateOdateResponse(string jsonData, ILambdaContext context, string key)
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
                using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
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
                context.Logger.LogLine(ex.ToString());
                return "Error";
            }
        }

    }
}