using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CopySQL
{
    class Program
    {

        private static StringBuilder Report { get; set; } = new StringBuilder();

        public static void Main(string[] args)
        {
            var mySqlConnectionString = ConfigurationManager.AppSettings["MySQLConnectionString"];
            var sqlServerConnectionString = ConfigurationManager.AppSettings["SQLServerConnectionString"];
            var tables = ConfigurationManager.AppSettings["Tables"].Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            var truncateBeforeInserting = ConfigurationManager.AppSettings["TruncateBeforeInserting"] == "true";
            var insertionBatch = int.Parse(ConfigurationManager.AppSettings["InsertionBatch"]);

            var tableReports = tables.Select(q => new TableReport()
            {
                Name = q,
            }).ToList();

            try
            {
                Console.WriteLine("Connecting to source SQL Server...");
                using (var mssqlConnection = new SqlConnection(sqlServerConnectionString))
                {
                    mssqlConnection.Open();
                    Console.WriteLine("Connecting to dest MySQL...");
                    using (var mySqlConnection = new MySqlConnection(mySqlConnectionString))
                    {
                        mySqlConnection.Open();

                        if (truncateBeforeInserting)
                        {
                            Console.WriteLine("Truncating the tables...");
                            foreach (var table in tables.Reverse())
                            {
                                Console.WriteLine("Disabling foreign key check");
                                mySqlConnection.ExecuteQuery("SET FOREIGN_KEY_CHECKS = 0;");
                                Report.AppendLine($"Disabled foreign key check.");

                                Console.WriteLine($" + Truncating {table}");
                                mySqlConnection.ExecuteQuery($"TRUNCATE {table}");
                                Report.AppendLine($"Truncated {table}");

                                Console.WriteLine("Enabling foreign key check");
                                mySqlConnection.ExecuteQuery("SET FOREIGN_KEY_CHECKS = 1;");
                                Report.AppendLine($"Enabled foreign key check.");
                            }
                        }

                        Console.WriteLine("Copying data...");

                        foreach (var table in tableReports)
                        {
                            Console.Write($" - Copying from {table.Name}... ");

                            // Count rows
                            table.RowCount = mssqlConnection.ExecuteLongScalarQuery($"SELECT COUNT(*) FROM {table.Name}");
                            Console.Write($"{table.RowCount} rows detected... ");

                            // Get the primary keys
                            var primaryKeys = mssqlConnection.GetPrimaryKeys(table.Name);
                            if (primaryKeys.Count() == 0)
                            {
                                Console.WriteLine("Skip, no primary key found...");
                                continue;
                            }

                            var primaryKeyOrderClause = string.Join(",", primaryKeys);

                            // Get already exist count
                            table.SkipCount = mySqlConnection.ExecuteLongScalarQuery($"SELECT COUNT(*) FROM {table.Name}");
                            if (table.SkipCount > 0)
                            {
                                Console.WriteLine($"Target already has {table.SkipCount}...");
                                table.RowCount -= table.SkipCount;
                            }

                            // Copy
                            table.CopiedRowCount = 0;
                            do
                            {
                                using (var selectCommand = new SqlCommand($"SELECT * FROM {table.Name} ORDER BY {primaryKeyOrderClause} OFFSET {table.SkipCount + table.CopiedRowCount} ROWS FETCH NEXT {insertionBatch} ROWS ONLY;", mssqlConnection))
                                {
                                    Console.WriteLine();
                                    Console.WriteLine($"Getting {insertionBatch} rows from SQL Server");

                                    #region Using Adapter 

                                    using (var adapter = new SqlDataAdapter (selectCommand))
                                    {
                                        using (var dataSet = new DataSet())
                                        {
                                            adapter.Fill(dataSet);
                                            var dataTable = dataSet.Tables[0];
                                            using (var streamWriter = new StreamWriter("dump.csv"))
                                            {
                                                Rfc4180Writer.WriteDataTable(dataTable, streamWriter, true);
                                            }

                                            var msbl = new MySqlBulkLoader(mySqlConnection)
                                            {
                                                TableName = table.Name,
                                                FileName = "dump.csv",
                                                FieldTerminator = ",",
                                                FieldQuotationCharacter = '"',
                                            };
                                            msbl.Load();

                                            File.Delete("dump.csv");

                                            table.CopiedRowCount += dataTable.Rows.Count;
                                            if (dataTable.Rows.Count == 0)
                                            {
                                                break;
                                            }
                                        }
                                    }

                                    #endregion

                                    #region Using Reader

                                    //using (var reader = selectCommand.ExecuteReader())
                                    //{
                                    //    var rowCount = 0;
                                    //    var paramCount = reader.FieldCount;

                                    //    //using (var transaction = mySqlConnection.BeginTransaction())
                                    //    //{
                                    //    //    using (var mySqlSelectCommand = new MySqlCommand($"SELECT * FROM {table.Name} WHERE 0 = 1 LIMIT 0, 1;", mySqlConnection, transaction))
                                    //    //    {
                                    //    //        using (var adapter = new MySqlDataAdapter(mySqlSelectCommand))
                                    //    //        {
                                    //    //            adapter.UpdateBatchSize = insertionBatch;

                                    //    //            using (var dataSet = new DataSet())
                                    //    //            {
                                    //    //                adapter.Fill(dataSet);
                                    //    //                var dataSetTable = dataSet.Tables[0];

                                    //    //                Console.WriteLine($"Inserting...");
                                    //    //                while (reader.Read())
                                    //    //                {
                                    //    //                    var dataRow = dataSetTable.NewRow();
                                    //    //                    for (int i = 0; i < paramCount; i++)
                                    //    //                    {
                                    //    //                        dataRow[i] = reader[i];
                                    //    //                    }

                                    //    //                    dataSetTable.Rows.Add(dataRow);

                                    //    //                    rowCount++;
                                    //    //                }

                                    //    //                var previous = DateTime.Now;
                                    //    //                Console.WriteLine("Sending to MySQL");
                                    //    //                using (var commandBuilder = new MySqlCommandBuilder(adapter))
                                    //    //                {
                                    //    //                    adapter.Update(dataSet);
                                    //    //                    transaction.Commit();

                                    //    //                    Console.WriteLine($"Done, took {(DateTime.Now - previous).TotalSeconds.ToString("0.0")}s");
                                    //    //                }
                                    //    //            }
                                    //    //        }
                                    //    //    }
                                    //    //}


                                    //    //using (var transaction = mySqlConnection.BeginTransaction())
                                    //    //{
                                    //    //    try
                                    //    //    {
                                    //    //        // Build the params used for insert queries
                                    //    //        var queryParamBuilder = new StringBuilder();
                                    //    //        for (int paramNo = 0; paramNo < paramCount; paramNo++)
                                    //    //        {
                                    //    //            queryParamBuilder.Append($"@p{paramNo},");
                                    //    //        }

                                    //    //        // Remove final comma ,
                                    //    //        queryParamBuilder.Remove(queryParamBuilder.Length - 1, 1);
                                    //    //        var queryParam = queryParamBuilder.ToString();

                                    //    //        Console.WriteLine($"Read result and insert into My SQL");
                                    //    //        while (reader.Read())
                                    //    //        {
                                    //    //            using (var insertCommand = new MySqlCommand($"INSERT INTO {table.Name} VALUES({queryParam})", mySqlConnection, transaction))
                                    //    //            {
                                    //    //                for (int i = 0; i < paramCount; i++)
                                    //    //                {
                                    //    //                    insertCommand.Parameters.AddWithValue($"@p{i}", reader[i]);
                                    //    //                }

                                    //    //                insertCommand.ExecuteNonQuery();
                                    //    //            }

                                    //    //            rowCount++;
                                    //    //        }

                                    //    //        Console.WriteLine($"Commit transaction");
                                    //    //        transaction.Commit();
                                    //    //    }
                                    //    //    catch (Exception)
                                    //    //    {
                                    //    //        transaction.Rollback();
                                    //    //        throw;
                                    //    //    }
                                    //    //}

                                    //    table.CopiedRowCount += rowCount;

                                    //    if (rowCount == 0)
                                    //    {
                                    //        break;
                                    //    }
                                    //}

                                    #endregion
                                }

                                Console.Write($"{table.CopiedRowCount} copied ({table.CopiedRowCount * 100 / table.RowCount}%)... ");

                            } while (true);
                            Console.WriteLine("Done!");
                            Console.WriteLine();
                        }
                    }
                }

                Console.WriteLine("Success!");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed:");
                Console.WriteLine(ex);
                goto ENDHERE;
            }

            ENDHERE:

            Console.WriteLine();
            Console.WriteLine(new string('-', 50));
            Console.WriteLine(Report.ToString());

            Console.WriteLine("Copied tables:");
            foreach (var tableReport in tableReports)
            {
                Console.WriteLine($" - {tableReport.Name}: {tableReport.CopiedRowCount} / {tableReport.RowCount}");
            }

            Console.ReadLine();
        }

    }

    public class TableReport
    {

        public string Name { get; set; }
        public long RowCount { get; set; }
        public long CopiedRowCount { get; set; }
        public long SkipCount { get; set; }

        public override string ToString()
        {
            return this.Name;
        }

    }

}
