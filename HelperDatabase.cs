using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace Prime.DataParser
{
    public static class HelperDatabase
    {
        private static readonly TimeSpan Delay = TimeSpan.FromSeconds(5);
        private static readonly Random Rnd = new Random();

        private static T Retry<T>(Func<T> func)
        {
            int count = 3;
            while (true)
            {
                try
                {
                    return func();
                }
                catch (SqlException sqlException)
                {
                    --count;
                    if (count <= 0) throw;

                    if (sqlException.Number == 1205)
                    {
                        Thread.Sleep(Rnd.Next(1000, 5000));
                    }
                    else if (sqlException.Number == -2)
                    {
                        // Timeout
                    }
                    else
                        throw;

                    Task.Delay(Delay);
                }
            }
        }

        public static void Execute(string connectionString, string commandString, SqlParameter[] parameters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            Retry(() =>
            {
                using (SqlConnection sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();
                    using (SqlCommand command = new SqlCommand(commandString, sqlConnection) { CommandType = commandType })
                    {
                        try
                        {
                            if (parameters != null) command.Parameters.AddRange(parameters);
                            command.ExecuteNonQuery();
                        }
                        finally
                        {
                            if (parameters != null) command.Parameters.Clear();
                        }
                    }
                }
                return true;
            });
        }

        public static SqlDataReader ExecuteReader(string connectionString, string commandString, SqlParameter[] parameters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            return Retry(() =>
            {
                SqlConnection conn = new SqlConnection(connectionString);
                using (SqlCommand cmd = new SqlCommand(commandString, conn))
                {
                    cmd.CommandType = commandType;
                    try
                    {
                        if (parameters != null) cmd.Parameters.AddRange(parameters);
                        conn.Open();

                        // When using CommandBehavior.CloseConnection, the connection will be closed when the IDataReader is closed.
                        return cmd.ExecuteReader(CommandBehavior.CloseConnection);
                    }
                    finally
                    {
                        if (parameters != null) cmd.Parameters.Clear();
                    }
                }
            });
        }

        public static DataTable ExecuteReaderToDataTable(string connectionString, string commandString, SqlParameter[] parameters = null, CommandType commandType = CommandType.Text, int commandTimeout = -1)
        {
            return Retry(() =>
            {
                DataTable dt = new DataTable();
                using (SqlConnection sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();
                    using (SqlCommand command = new SqlCommand(commandString, sqlConnection) { CommandType = commandType })
                    {
                        if (commandTimeout != -1) command.CommandTimeout = commandTimeout;
                        try
                        {
                            if (parameters != null) command.Parameters.AddRange(parameters);
                            using (SqlDataReader reader = command.ExecuteReader()) dt.Load(reader);
                        }
                        finally
                        {
                            if (parameters != null) command.Parameters.Clear();
                        }
                    }
                }
                return dt;
            });
        }

        public static DataSet ExecuteAdapter(string connectionString, string commandString, SqlParameter[] parameters = null)
        {
            return Retry(() =>
            {
                DataSet dataset = new DataSet();
                using (SqlConnection sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();
                    SqlDataAdapter adapter = new SqlDataAdapter();
                    adapter.SelectCommand = new SqlCommand(commandString, sqlConnection);
                    adapter.SelectCommand.CommandType = CommandType.StoredProcedure;
                    if (parameters != null) adapter.SelectCommand.Parameters.AddRange(parameters);
                    adapter.Fill(dataset);
                }
                return dataset;
            });
        }

        public static T ExecuteSingleReader<T>(string connectionString, string commandString, Func<SqlDataReader, T> action, SqlParameter[] parameters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            return ExecuteSingleReader(connectionString, commandString, parameters, action, commandType);
        }

        public static T ExecuteSingleReader<T>(string connectionString, string commandString, SqlParameter[] parameters, Func<SqlDataReader, T> action, CommandType commandType = CommandType.StoredProcedure)
        {
            return Retry(() =>
            {
                using (SqlConnection sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();
                    using (SqlCommand command = new SqlCommand(commandString, sqlConnection) { CommandType = commandType })
                    {
                        try
                        {
                            if (parameters != null) command.Parameters.AddRange(parameters);
                            using (SqlDataReader reader = command.ExecuteReader()) if (reader.Read()) return action(reader);
                        }
                        finally
                        {
                            if (parameters != null) command.Parameters.Clear();
                        }
                    }
                }
                return default(T);
            });
        }

        public static T ExecuteScalar<T>(string connectionString, string commandString, SqlParameter[] parameters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            return Retry(() =>
            {
                using (SqlConnection sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();
                    using (SqlCommand command = new SqlCommand(commandString, sqlConnection) { CommandType = commandType })
                    {
                        try
                        {
                            if (parameters != null) command.Parameters.AddRange(parameters);

                            object value = command.ExecuteScalar();
                            if (value is DBNull || value == null) return default(T);
                            return (T)Convert.ChangeType(value, typeof(T));
                        }
                        finally
                        {
                            if (parameters != null) command.Parameters.Clear();
                        }
                    }
                }
            });
        }

        public static void ExecuteScalar(string connectionString, string commandString, SqlParameter[] parameters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            Retry(() =>
            {
                using (SqlConnection sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();
                    using (SqlCommand command = new SqlCommand(commandString, sqlConnection) { CommandType = commandType })
                    {
                        try
                        {
                            if (parameters != null) command.Parameters.AddRange(parameters);

                            command.ExecuteScalar();
                            return true;
                        }
                        finally
                        {
                            if (parameters != null) command.Parameters.Clear();
                        }
                    }
                }
            });
        }
    }
}
