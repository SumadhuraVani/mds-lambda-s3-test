using System;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DataModel;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Linq;
using System.Net;
using Amazon;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace MDS_RSMap_Source
{
    public class RsMappings
    {
        public string OID { get; set; }
        public string OIDCIQ { get; set; }

        public DateTime PublishDate { get; set; }
    }
    public class Function
    {
        private static string tableName = "MDSSourceDynamoDBTest1";
        private static string DestinationtableName = "MDSSourceDynamoDBDest1";
        private static readonly JsonSerializer _jsonSerializer = new JsonSerializer();
        static int maxRetryAttempts = 3;
        static TimeSpan pauseBetweenFailures = TimeSpan.FromSeconds(2);
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient(RegionEndpoint.USEast1);
        private static string Domain = "snl";
        private static string Password = "R0sencrantz";
        private static string UserName = "imageservice";
        private static string RSMappedData = "KeyRatingCreditType";
        private static string RSMappedQuery = "GMISNLCIQMappings?select=OID,OIDCIQ&$filter=(({0}) and KeyTable eq {1} and KeyMINameSpace eq 5)&editable=true";
        private static string URLData = "http://chomisvcdev.snl.int/SNL.Services.Data.Api.Service/v2/Internal/Editable/";
        private static string RSMapTableValue = "10197";
        //private static string RSMappedData = Environment.GetEnvironmentVariable("RSMappedData");
        //private static string RSMappedQuery = Environment.GetEnvironmentVariable("RSMappedQuery");
        //private static string URL = Environment.GetEnvironmentVariable("URL");
        //private static string RSMapTableValue = Environment.GetEnvironmentVariable("RSMapTableValue");
        // private static string tableName = Environment.GetEnvironmentVariable("SourceDynamoDB");
        // private static string tableName = "MDSDynamoSource";//Need to move to env
        //private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        // private static string DestinationtableName = Environment.GetEnvironmentVariable("DestinationDynamoDB");
        public StringBuilder GetallRecordsWhereClause(List<string> Values)
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

        private static async Task<string> CreateTableItem(Table Catalog, Document docData, ILambdaContext context, string DataModified)
        {
            try
            {
                //context.Logger.Log("Vani Before Insertion");
                var data = new Document();
                data["GUID"] = docData["guid"].ToString();
                // context.Logger.Log("Vani Before Insertion1");
                data["Data"] = DataModified;
                // context.Logger.Log("Vani Before Insertion2");
                data["updatedDate"] = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssf"));
                data["timestamp"] = Convert.ToInt64(docData["timestamp"].ToString());
                data["UniqueRow"] = docData["UniqueRow"];
                data["Error"] = docData["Error"];
                Document response = await Catalog.PutItemAsync(data);
                context.Logger.Log("RS Mapped Data inserted in destination dynamo");
                return "Success";
            }
            catch (Exception ex)
            {
                context.Logger.Log("Error" + ex.Message);
                return "Failure";
            }
        }
        public JToken ExecuteODataGetQuery(string ODataQueryFilter, ILambdaContext context)
        {
            try
            {
                string ODataErrorResponse = string.Empty;
                AppContext.SetSwitch("System.Net.Http.UseSocketsHttpHandler", false);               
                string URL = URLData + ODataQueryFilter;
                string responseStr = string.Empty;
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
        public void ProcessData(List<Document> documentList,ILambdaContext context)
        { }

        public async Task<string> FunctionHandler(DynamoDBEvent dynamoEvent, ILambdaContext context)
        {
            try
            {
                List<RsMappings> rsMappings = new List<RsMappings>();
                Task<string> Response = null;
                string DataModified = string.Empty;
                string ResponseData = string.Empty;
                Table Catolog = Table.LoadTable(client, tableName);
                ScanFilter scan = new ScanFilter();
                string RSMapped = RSMappedData;
                string[] RSMappedSplit = RSMapped.Split(',').ToArray();//for sector
                //List<string> GetRSData = new List<string>();
                Dictionary<string, string> GetRSData = new Dictionary<string, string>();
                scan.AddCondition("guid", ScanOperator.IsNotNull);
                Search search = Catolog.Scan(scan);
                bool isNumeric = false;
                string[] RSMappdata = null;
                int RSMapCount = 0;
                List<Document> documentList = new List<Document>();
                //documentList = documentList.OrderBy(x=>x["ID"]).ToList();         
                do
                {
                    try
                    {
                        documentList = await search.GetRemainingAsync();//.GetNextSetAsync();

                        foreach (Document doc in documentList)
                        {
                            RSMapCount = 0;
                            if (!string.IsNullOrEmpty(doc["RSMappedValues"].ToString()))
                            {
                                RSMappdata = doc["RSMappedValues"].ToString().Split('#').ToArray();
                                foreach (string str in RSMappdata)
                                {
                                    isNumeric = int.TryParse(str, out int i);
                                    if (!GetRSData.ContainsKey(str) && isNumeric == false && !str.Equals("--"))
                                        GetRSData.Add(str, RSMappedSplit.ElementAt(RSMapCount));
                                    RSMapCount++;
                                }
                            }
                            //context.Logger.LogLine("test 4");           
                        }
                    }
                    catch(Exception ex)
                    {

                    }
                } while (!search.IsDone);
                context.Logger.LogLine("Data read from dynamo DB");
                for (int i = 0; i < RSMappedSplit.Count(); i++)
                {
                    List<string> RSMappedDataAfterSplit = GetRSData.Where(x => x.Value.Equals(RSMappedSplit.ElementAt(i))).Select(y => y.Key).ToList();
                    var whereClause = GetallRecordsWhereClause(RSMappedDataAfterSplit);
                    Catolog = Table.LoadTable(client, DestinationtableName);
                    context.Logger.LogLine(whereClause.ToString());
                    if (GetRSData != null && GetRSData.Count > 0)
                    {
                        string RSmapquery = string.Format(RSMappedQuery, whereClause, RSMapTableValue.Split(',')[i]);

                        var ODataRsponse1 = ExecuteODataGetQuery(RSmapquery, context);
                        if (ODataRsponse1 != null && ODataRsponse1.HasValues)
                            rsMappings.AddRange(ODataRsponse1.ToObject<List<RsMappings>>());
                        context.Logger.Log(rsMappings.Count.ToString());
                    }
                }

                foreach (Document doc in documentList)
                {
                    context.Logger.Log("heree");
                    int rsmapped = 0;
                    JObject jsonData = JObject.Parse(doc["Data"]);
                    string guidValue = doc["guid"].ToString();
                    dynamic dynamoClass = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(doc["Data"].ToString());
                    for (int j = 0; j < RSMappedSplit.Count(); j++)
                    {
                        isNumeric = int.TryParse(doc["RSMappedValues"].ToString().Split('#')[j], out int i);
                        if (isNumeric == false)
                        {
                            var RSData = rsMappings != null ? rsMappings.FirstOrDefault(mapping => mapping.OIDCIQ == jsonData.SelectToken(RSMappedSplit.ElementAt(j)).ToString()) : null;
                            if (RSData != null)
                            {
                                dynamoClass[RSMappedSplit.ElementAt(j)].Value = RSData.OID;
                                rsmapped++;
                            }
                        }
                        else
                        {
                            rsmapped++;
                        }
                    }
                    if (rsmapped == RSMappedSplit.Count())
                    {
                        ResponseData = await CreateTableItem(Catolog, doc, context, dynamoClass.ToString());
                        context.Logger.LogLine(ResponseData);
                        if (ResponseData.Equals("Success"))
                        {
                            DeleteItemRequest request = new DeleteItemRequest();
                            request.TableName = tableName;
                            request.Key = new Dictionary<string, AttributeValue>() { { "guid", new AttributeValue { S = guidValue } } };

                            context.Logger.LogLine("Del setup");
                            var response = await client.DeleteItemAsync(request);
                        }
                    }
                }

                context.Logger.LogLine("Data Loaded Successfully");
                return "Success";
            }

            catch (Exception ex)
            {
                context.Logger.LogLine("Error" + ex.ToString());
                return null;
            }
        }


    }
}