using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace fnSQL
{
    public static class covid
    {
        [FunctionName("covid")]
        public static IActionResult Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string country = req.Query["country"];
            string connStr = Environment.GetEnvironmentVariable("SynapseServerless");
            IEnumerable<Dictionary<string, object>> result;

            using (SqlConnection conn = new SqlConnection(connStr))
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    SqlDataReader dataReader;

                    string strSQL = "SELECT * FROM vCOVID";

                    if (!(String.IsNullOrEmpty(country)))
                    {
                        strSQL = strSQL + " WHERE Country = @country";
                        cmd.Parameters.AddWithValue("@country", country);
                    }

                    cmd.CommandText = strSQL;
                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = conn;
                    conn.Open();
                    dataReader = cmd.ExecuteReader();
                    result = Serialize(dataReader);
                };
            }

            return new JsonResult(result, new JsonSerializerSettings { Formatting = Formatting.Indented });
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
