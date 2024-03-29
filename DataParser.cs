using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Prime.DataParser
{
    public static class DataParser
    {
        /// <summary>
        /// Returns array of JSON resultset
        /// </summary>
        /// <param name="sqlConnectionString">Set the string to open SQL Server database</param>
        /// <param name="procedureName">stored procedure to call to retrieve desire result</param>
        /// <param name="sqlParams">array of parameters to pass to stored procedure </param>
        public static string ParseSingleSet(string sqlConnectionString, string procedureName, Dictionary<string, object> sqlParams = null)
        {
            Dictionary<string, object> queryParams = new Dictionary<string, object>();

            List<SqlParameter> parms = new List<SqlParameter>();
            if (sqlParams != null) queryParams = sqlParams;
            foreach (var pair in queryParams)
            {
                SqlParameter item = new SqlParameter
                {
                    ParameterName = pair.Key,
                    Value = pair.Value
                };
                parms.Add(item);
            }
            SqlDataReader dr = HelperDatabase.ExecuteReader(GetConnectionString(sqlConnectionString), procedureName, parms.ToArray());
            return JsonConverter.Serailize(dr);
        }

        /// <summary>
        /// Returns array of JSON resultset
        /// </summary>
        /// <param name="sqlConnectionString">Set the string to open SQL Server database</param>
        /// <param name="procedureName">stored procedure to call to retrieve desire result</param>
        /// <param name="sqlParams">array of parameters to pass to stored procedure </param>
        /// <param name="expectedTables">expected tables that return from stored procedure</param>
        /// <param name="tableMappings">array to name the inner arrays in JSON object</param>
        public static string ParseMultiLevelSets(string sqlConnectionString, string procedureName, int expectedTables, JsonConverter.TableMapping[] tableMappings, Dictionary<string, object> sqlParams = null)
        {
            Dictionary<string, object> queryParams = new Dictionary<string, object>(sqlParams);
            List<SqlParameter> parms = new List<SqlParameter>();
            if (sqlParams != null) queryParams = sqlParams;
            foreach (var pair in queryParams)
            {
                SqlParameter item = new SqlParameter
                {
                    ParameterName = pair.Key,
                    Value = pair.Value
                };
                parms.Add(item);
            }
            SqlDataReader dr = HelperDatabase.ExecuteReader(GetConnectionString(sqlConnectionString), procedureName, parms.ToArray());
            return JsonConverter.SerailizeMultiLevel(dr, expectedTables, tableMappings);
        }
        /// <summary>
        /// Returns dataset
        /// </summary>
        /// <param name="sqlConnectionString">Set the string to open SQL Server database</param>
        /// <param name="procedureName">stored procedure to call to retrieve desire result</param>
        /// <param name="sqlParams">array of parameters to pass to stored procedure </param>

        public static DataSet ParseDataSet(string sqlConnectionString, string procedureName, Dictionary<string, object> sqlParams = null)
        {
            Dictionary<string, object> queryParams = new Dictionary<string, object>(sqlParams);
            List<SqlParameter> parms = new List<SqlParameter>();
            if (sqlParams != null) queryParams = sqlParams;
            foreach (var pair in queryParams)
            {
                SqlParameter item = new SqlParameter
                {
                    ParameterName = pair.Key,
                    Value = pair.Value
                };
                parms.Add(item);
            }
            DataSet ds = HelperDatabase.ExecuteAdapter(GetConnectionString(sqlConnectionString), procedureName, parms.ToArray());
            return ds;
        }

        private static string GetConnectionString(string connectionString)
        {
            SqlConnectionStringBuilder sqlConnectionStringBuilder = new SqlConnectionStringBuilder(connectionString)
            {
                MaxPoolSize = 100,
                Pooling = true
            };
            return sqlConnectionStringBuilder.ConnectionString;
        }
    }
}
