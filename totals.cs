using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Net.Http;
using System.Text;

namespace fnSQL
{
    public static class totals
    {
        [FunctionName("totals")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequestMessage req, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            var connStr = Environment.GetEnvironmentVariable("SynapseServerless");
            string json = "";

            using (SqlConnection conn = new SqlConnection(connStr))

            {
                using (SqlCommand cmd = new SqlCommand())

                {
                    SqlDataReader dataReader;

                    cmd.CommandText = "SELECT Country, Confirmed = MAX(Confirmed), Recovered = MAX(Recovered), Deaths = MAX(Deaths) FROM vCOVID GROUP BY Country ORDER BY Country";

                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = conn;
                    conn.Open();
                    dataReader = cmd.ExecuteReader();
                    var r = Serialize(dataReader);
                    json = JsonConvert.SerializeObject(r, Formatting.Indented);
                };

                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(json, Encoding.UTF8, "application/json")
                };
            }
        }

        private static IEnumerable<Dictionary<string, object>> Serialize(SqlDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            var cols = new List<string>();

            for (var i = 0; i < reader.FieldCount; i++)
            {
                var colName = reader.GetName(i);
                cols.Add(colName);
            }

            while (reader.Read())
            {
                results.Add(SerializeRow(cols, reader));
            }

            return results;
        }

        private static Dictionary<string, object> SerializeRow(IEnumerable<string> cols, SqlDataReader reader)
        {
            var result = new Dictionary<string, object>();

            foreach (var col in cols)
            {
                result.Add(col, reader[col]);
            }

            return result;
        }
    }
}
