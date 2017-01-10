using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CopySQL
{

    public static class Extensions
    {

        public static int ExecuteQuery(this MySqlConnection connection, string query)
        {
            using (var command = new MySqlCommand(query, connection))
            {
                return command.ExecuteNonQuery();
            }
        }

        public static long ExecuteLongScalarQuery(this SqlConnection connection, string query)
        {
            using (var command = new SqlCommand(query, connection))
            {
                return Convert.ToInt64(command.ExecuteScalar());
            }
        }

        public static IEnumerable<string> GetPrimaryKeys(this SqlConnection connection, string tableName)
        {
            var query = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND TABLE_NAME = @tableName ORDER BY ORDINAL_POSITION;";
            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@tableName", tableName);
                var result = new List<string>();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader[0] as string);
                    }
                }

                return result;
            }
        }

    }

}
