using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Prime.DataParser
{
    public static class JsonConverter
    {
        private static readonly JsonSerializerSettings JsonSerializerSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };

        /// <summary>
        /// Returns array of JSON resultset
        /// </summary>
        /// <param name="reader">reader to convert</param>
        /// <param name="searchColumn">ForiegnKey column to map</param>
        /// <param name="detailArrayName">array to name the inner arrays in JSON object</param>
        public static string SerailizeDetail(SqlDataReader reader, string searchColumn, string[] detailArrayName)
        {
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            string filteredVal = "";
            int i = 1;
            List<DataTable> tables = new List<DataTable>();
            DataTable dt = ConvertToDataTable(reader);
            tables.Add(dt);
            while (reader.NextResult())
            {
                dt = ConvertToDataTable(reader);
                tables.Add(dt);
            }
            if (tables.Count >= 1)
            {
                foreach (DataRow dr in tables[0].Rows)
                {
                    Dictionary<string, object> row = new Dictionary<string, object>();
                    foreach (DataColumn col in tables[0].Columns)
                    {
                        row.Add(col.ColumnName, dr[col]);
                        if (col.ColumnName == searchColumn)
                            filteredVal = dr[col].ToString();
                    }
                    for (; i <= tables.Count - 1; i++)
                    {
                        var objArray = AddChild(tables[i], searchColumn, filteredVal);
                        var detailArray = JsonConvert.DeserializeObject(objArray);
                        row.Add(detailArrayName[i - 1], detailArray);
                    }
                    i = 1;
                    rows.Add(row);
                }
            }
            return JsonConvert.SerializeObject(rows, Formatting.Indented, JsonSerializerSettings);
        }

        private static string AddChild(DataTable dt, string column, string value)
        {
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            DataRow[] foundRows = dt.Select(column + "='" + value + "'");
            foreach (DataRow dr in foundRows)
            {
                Dictionary<string, object> row = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                    row.Add(col.ColumnName, dr[col]);
                rows.Add(row);
            }
            return JsonConvert.SerializeObject(rows, Formatting.Indented, JsonSerializerSettings);
        }

        private static string AddChildren(DataTable[] dt, string[] columnList, string value, string arrName)
        {
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            DataRow[] foundRows = dt[0].Select(columnList[0] + "='" + value + "'");
            string filteredVal = "";
            foreach (DataRow dr in foundRows)
            {
                Dictionary<string, object> row = new Dictionary<string, object>();
                foreach (DataColumn col in dt[0].Columns)
                {
                    row.Add(col.ColumnName, dr[col]);
                    if (col.ColumnName == columnList[1]) filteredVal = dr[col].ToString();
                }
                var objArray = AddChild(dt[1], columnList[1], filteredVal);
                var detailArray = JsonConvert.DeserializeObject(objArray);
                row.Add(arrName, detailArray);
                rows.Add(row);
            }
            return JsonConvert.SerializeObject(rows, Formatting.Indented, JsonSerializerSettings);
        }

        /// <summary>
        /// Returns array of JSON resultset
        /// </summary>
        /// <param name="reader"></param>
        public static string Serailize(SqlDataReader reader)
        {
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            DataTable resultSet = new DataTable();
            resultSet.Load(reader);
            foreach (DataRow dr in resultSet.Rows)
            {
                Dictionary<string, object> row = new Dictionary<string, object>();
                foreach (DataColumn col in resultSet.Columns)
                    row.Add(col.ColumnName, dr[col]);
                rows.Add(row);
            }
            return JsonConvert.SerializeObject(rows, Formatting.Indented, JsonSerializerSettings);
        }

        private static DataTable ConvertToDataTable(SqlDataReader dr)
        {
            DataTable dtSchema = dr.GetSchemaTable();
            DataTable dt = new DataTable();
            List<DataColumn> listCols = new List<DataColumn>();
            if (dtSchema != null)
            {
                foreach (DataRow drow in dtSchema.Rows)
                {
                    string columnName = Convert.ToString(drow["ColumnName"]);
                    DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
                    listCols.Add(column);
                    dt.Columns.Add(column);
                }
            }
            while (dr.Read())
            {
                DataRow dataRow = dt.NewRow();
                for (int i = 0; i < listCols.Count; i++)
                    dataRow[listCols[i]] = dr[i];
                dt.Rows.Add(dataRow);
            }
            return dt;
        }

        public class TableMapping
        {
            public int SourceTableIndex;
            public int DestinationTableIndex;
            public string DestinationCollectionName;
            public string MasterFieldName;
        }

        public class TableResults
        {
            public List<Dictionary<string, object>> Rows;
            public Dictionary<string, Dictionary<object, List<object>>> ParentDetailLineLookup;
        }

        /// <summary>
        /// Returns array of JSON resultset
        /// </summary>
        /// <param name="reader">reader to convert</param>
        /// <param name="expectedTables">expected tables from return sql</param>
        /// <param name="tableMappings">array to name the inner arrays in JSON object</param>
        public static string SerailizeMultiLevel(SqlDataReader reader, int expectedTables, TableMapping[] tableMappings)
        {
            List<DataTable> tables = new List<DataTable>();
            for (int tableId = 0; tableId < expectedTables; tableId++)
            {
                DataTable dataTable = new DataTable();
                dataTable.Load(reader);
                tables.Add(dataTable);
            }
            // if our source is our destination then its the base table to populate
            TableMapping baseTable = tableMappings.FirstOrDefault(mapping => mapping.SourceTableIndex == mapping.DestinationTableIndex);
            if (baseTable == null)
                throw new InvalidConstraintException("Missing base table");
            TableResults results = PopulateTableFromChildren(tables, baseTable, tableMappings);
            return JsonConvert.SerializeObject(tables, Formatting.Indented, JsonSerializerSettings);
        }

        public static TableResults PopulateTableFromChildren(List<DataTable> tables, TableMapping masterTable, TableMapping[] tableMappings, Dictionary<string, List<string>> keyFields = null)
        {
            // Populate our object we will serialize in the end
            DataColumnCollection masterColumns = tables[masterTable.SourceTableIndex].Columns;

            // Dictionary of grouping field, list of destination collection names that will be created
            Dictionary<string, List<string>> detailKeyFields = new Dictionary<string, List<string>>();

            // First we need to tell if anyone is wanting to add detail lines to us (the master)
            //foreach (DataColumn masterColumn in masterColumns)
            //{
            //    string masterColumnName = masterColumn.ColumnName.ToLower();

            //    // Now find any sub table which has a grouping on this field since we need to construct a detail line for this
            //    tableMappings.Where(childTable =>
            //        childTable.DestinationTableIndex == masterTable.SourceTableIndex // Detail table points to Master
            //        && childTable.MasterFieldName != null
            //        && childTable.MasterFieldName.ToLower() == masterColumnName // Is the master column used in a detail grouping or does it need one at all
            //        && childTable.SourceTableIndex != masterTable.SourceTableIndex) // Its not the same table
            //        .ToList()
            //        .ForEach(tableMapping =>
            //        {
            //            if (!detailKeyFields.ContainsKey(masterColumnName))
            //                detailKeyFields[masterColumnName] = new List<string>();

            //            // Add the new column with the List of child rows
            //            detailKeyFields[masterColumnName].Add(tableMapping.DestinationCollectionName);
            //        });
            //}

            Dictionary<string, TableResults> detailResults = new Dictionary<string, TableResults>();

            // if there are no children that need to be constructed from us then we no longer need to nest
            if (detailKeyFields.Count != 0)
            {
                // Now find any sub table which has a grouping on this field since we need to construct a detail line for this
                foreach (TableMapping detailTableMapping in tableMappings.Where(childTable =>
                    childTable.DestinationTableIndex == masterTable.SourceTableIndex // Detail table points to Master
                    && childTable.SourceTableIndex != masterTable.SourceTableIndex)) // Its not the same table
                {
                    // Add the new column with the List of child rows
                    detailResults[detailTableMapping.DestinationCollectionName] = PopulateTableFromChildren(tables, detailTableMapping, tableMappings, detailKeyFields);
                }
            }

            List<Dictionary<string, object>> masterResults = new List<Dictionary<string, object>>();

            // Dictionary of Field that is grouped,
            //    Dictionary of grouped value
            //       List of rows (Dictionary of column, value)
            Dictionary<string, Dictionary<object, List<object>>> parentDetailLineLookup = new Dictionary<string, Dictionary<object, List<object>>>();

            // Loop through the master table building up a dictionary of master/detail rows
            foreach (DataRow masterDataRow in tables[masterTable.SourceTableIndex].Rows)
            {
                Dictionary<string, object> masterRow = new Dictionary<string, object>();
                foreach (DataColumn masterColumn in masterColumns)
                {
                    string masterColumnName = masterColumn.ColumnName.ToLower();
                    masterRow[masterColumn.ColumnName] = masterDataRow[masterColumn];

                    // do we have any collections the need to be added to this data row
                    if (detailKeyFields.ContainsKey(masterColumnName))
                    {
                        // for each of the collections we need to create for this column
                        foreach (string destinationCollectionName in detailKeyFields[masterColumnName])
                        {
                            object collectionValue = masterDataRow[masterColumn];

                            // is there any results for this new column in our row
                            // Does the collection from the child contain the grouped field and does it match the value we have
                            if (detailResults.Count > 0 &&
                                detailResults.ContainsKey(destinationCollectionName) &&
                                detailResults[destinationCollectionName].ParentDetailLineLookup.ContainsKey(masterColumnName) &&
                                detailResults[destinationCollectionName].ParentDetailLineLookup[masterColumnName].ContainsKey(collectionValue))
                            {
                                masterRow[destinationCollectionName] = detailResults[destinationCollectionName].ParentDetailLineLookup[masterColumnName][collectionValue];
                            }
                            else
                            {
                                masterRow[destinationCollectionName] = new List<object>();
                            }
                        }
                    }
                }

                if (keyFields != null)
                {
                    // if this columns is a key field for the master of me then collect it up for them
                    foreach (string keyField in keyFields.Keys)
                    {
                        // Add first level which is the grouping name to collect up
                        if (!parentDetailLineLookup.ContainsKey(keyField))
                            parentDetailLineLookup[keyField] = new Dictionary<object, List<object>>();

                        // Now add the unique value to the collection
                        if (!parentDetailLineLookup[keyField].ContainsKey(masterDataRow[keyField]))
                            parentDetailLineLookup[keyField][masterDataRow[keyField]] = new List<object>();

                        parentDetailLineLookup[keyField][masterDataRow[keyField]].Add(masterRow);
                    }
                }

                masterResults.Add(masterRow);
            }

            return new TableResults { ParentDetailLineLookup = parentDetailLineLookup, Rows = masterResults };
        }
    }
}
