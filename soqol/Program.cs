using System.Data.Odbc;
using System.Text;

namespace SoQoL
{
    internal class Program
    {
        private readonly static string DSN_SERVICE =
            "DSN=soqol;SERVER=127.0.0.1;PORT=2060;UID=SOQOL;PWD=SOQOL;";
        private readonly static string DSN_DATABASE =
            "DSN=soqol;SERVER=127.0.0.1;PORT=2060;DATABASE=test;UID=SOQOL;PWD=SOQOL;";
        
        private readonly static string CREATE_DATABASE = "create database test on './test';";
        private readonly static string SHUTDOWN_DATABASE = "shutdown database test;";
        private readonly static string DROP_DATABASE = "drop database test;";

        private readonly static string DROP_TABLE = "drop table test;";
        private readonly static string CREATE_TABLE =
            "create table test (f_bln boolean, f_num numeric(10,0), " +
            "f_dtm datetime, f_str varchar(100), f_bin varbinary(100));";

        private readonly static string INSERT_OUTPUT_COMMAND =
            "insert into test (f_bln,  f_num,  f_dtm,  f_str,  f_bin) " +
            "         values (:f_bln, :f_num, :f_dtm, :f_str, :f_bin) " +
            "      returning   f_bln,  f_num,  f_dtm,  f_str,  f_bin;";

        static void Main()
        {
            TryExecute(CreateDatabase);
            TryExecute(CreateTable);

            // Тест на запись/чтение/сравнение
            // значений основных типов данных:
            // 1. boolean   > Success
            // 2. numeric   > Success
            // 3. datetime  > ERROR (отсечена часть hh:mm:ss)
            // 4. varchar   > ERROR (отсечён последний символ)
            // 5. varbinary > Success
            TryExecute(WriteReadCompareTest);

            TryExecute(DropTable);
            TryExecute(DropDatabase);
        }
        private static void TryExecute(Action action)
        {
            try
            {
                action();
            }
            catch (Exception error)
            {
                Console.WriteLine($"ERROR: {error.Message}");
            }
        }
        private static void CreateDatabase()
        {
            using (OdbcConnection connection = new(DSN_SERVICE))
            {
                connection.Open();

                using (OdbcCommand command = connection.CreateCommand())
                {
                    command.CommandText = CREATE_DATABASE;
                    _ = command.ExecuteNonQuery();
                    Console.WriteLine("Success: " + CREATE_DATABASE);
                }
            }
        }
        private static void CreateTable()
        {
            using (OdbcConnection connection = new(DSN_DATABASE))
            {
                connection.Open();

                using (OdbcCommand command = connection.CreateCommand())
                {
                    command.CommandText = CREATE_TABLE;
                    _ = command.ExecuteNonQuery();
                    Console.WriteLine("Success: " + CREATE_TABLE);
                }
            }
        }
        private static void DropTable()
        {
            using (OdbcConnection connection = new(DSN_DATABASE))
            {
                connection.Open();

                using (OdbcCommand command = connection.CreateCommand())
                {
                    command.CommandText = DROP_TABLE;
                    _ = command.ExecuteNonQuery();
                    Console.WriteLine("Success: " + DROP_TABLE);
                }
            }
        }
        private static void DropDatabase()
        {
            using (OdbcConnection connection = new(DSN_SERVICE))
            {
                connection.Open();

                using (OdbcCommand command = connection.CreateCommand())
                {
                    command.CommandText = SHUTDOWN_DATABASE;
                    _ = command.ExecuteNonQuery();
                    Console.WriteLine("Success: " + SHUTDOWN_DATABASE);

                    command.CommandText = DROP_DATABASE;
                    _ = command.ExecuteNonQuery();
                    Console.WriteLine("Success: " + DROP_DATABASE);
                }
            }
        }
        private static void WriteReadCompareTest()
        {
            Dictionary<string, object> values = new()
            {
                ["f_bln"] = true,
                ["f_num"] = 12345M,
                ["f_dtm"] = new DateTime(1234, 5, 6, 7, 8, 9),
                ["f_str"] = "тест string",
                ["f_bin"] = Encoding.UTF8.GetBytes("UTF-8 строка")
            };

            using (OdbcConnection connection = new(DSN_DATABASE))
            {
                connection.Open();

                using (OdbcCommand command = connection.CreateCommand())
                {
                    command.CommandText = INSERT_OUTPUT_COMMAND;

                    command.Parameters.Add("f_bln", OdbcType.Bit).Value = values["f_bln"];
                    command.Parameters.Add("f_num", OdbcType.Numeric).Value = values["f_num"];
                    command.Parameters.Add("f_dtm", OdbcType.DateTime).Value = values["f_dtm"];
                    command.Parameters.Add("f_str", OdbcType.NVarChar).Value = values["f_str"];
                    command.Parameters.Add("f_bin", OdbcType.VarBinary).Value = values["f_bin"];

                    using (OdbcDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                if (!reader.IsDBNull(i))
                                {
                                    string name = reader.GetName(i).ToLowerInvariant();
                                    object value = reader.GetValue(i);
                                    Type type = reader.GetFieldType(i);

                                    if (type == typeof(byte[]) && values[name] is byte[] test && value is byte[] data)
                                    {
                                        if (test.SequenceEqual(data))
                                        {
                                            Console.WriteLine($"Success [{type}]: {Encoding.UTF8.GetString(test)} == {Encoding.UTF8.GetString(data)}");
                                        }
                                        else
                                        {
                                            Console.WriteLine($"Error [{type}]: {BitConverter.ToString(test)} <> {BitConverter.ToString(data)}");
                                        }
                                    }
                                    else if (values[name].Equals(value))
                                    {
                                        Console.WriteLine($"Success [{type}]: {values[name]} == {value}");
                                    }
                                    else
                                    {
                                        Console.WriteLine($"Error [{type}]: {values[name]} <> {value}");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}