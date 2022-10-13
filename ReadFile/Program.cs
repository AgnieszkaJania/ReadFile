using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ReadFile
{   public static class Constants
    {
        public const string  connectionString = "Data Source=(LocalDB)\\MSSQLLocalDB;Initial Catalog=WORK_TASK;Integrated Security=True";
        public const string url = "https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv";
    }
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                HttpWebRequest req = (HttpWebRequest)WebRequest.Create(Constants.url);
                HttpWebResponse resp = (HttpWebResponse)req.GetResponse();
                Stream sr = resp.GetResponseStream();

                TextFieldParser textFieldParser = new TextFieldParser(sr);
                textFieldParser.TextFieldType = FieldType.Delimited;
                textFieldParser.SetDelimiters(",");
                textFieldParser.HasFieldsEnclosedInQuotes = true;

                int lineNumber = 0;
                var dt = new DataTable("DataFromCSV");

                while (!textFieldParser.EndOfData)
                {
                    string[] fields = textFieldParser.ReadFields();
                    if (lineNumber == 0)
                    {
                        foreach (var column in fields)
                        {
                            dt.Columns.Add(column.Trim(), typeof(string));
                        }

                    }
                    if (lineNumber > 0)
                    {
                        dt.Rows.Add(fields);
                    }
                    lineNumber++;
                }

                CreateTableInDB();
                using (var connection = new SqlConnection(Constants.connectionString))
                {
                    connection.Open();
                    using (var sqlBulk = new SqlBulkCopy(connection))
                    {
                        sqlBulk.DestinationTableName = "DataFromCSV";
                        sqlBulk.WriteToServer(dt);
                    }
                }
                Console.WriteLine("Finished!");
                Console.ReadKey();
            }
            catch(SqlException e)
            {
                Console.WriteLine($"SQL Server returned an error. Message: {e.Message}");
            }
            catch (MalformedLineException e)
            {
                Console.WriteLine($"Input row data is malformed. Message: {e.Message} ");
            }
            catch (Exception e)
            {

                Console.WriteLine($"An error occurred. Message: {e.Message} ");
            }

        }   
        public static void CreateTableInDB()
        {
            string sql = @"
                IF EXISTS (SELECT * FROM sys.tables WHERE name = 'DataFromCSV')
                BEGIN
                    TRUNCATE TABLE DataFromCSV
                END
                ELSE
                BEGIN
                    CREATE TABLE DataFromCSV
                        (
                            Year varchar(4),
                            Industry_aggregation_NZSIOC varchar(255),
                            Industry_code_NZSIOC varchar(255),
                            Industry_name_NZSIOC varchar(255),
                            Units varchar(255),
                            Variable_code varchar(255),
                            Variable_name varchar(255),
                            Variable_category varchar(255),
                            Value varchar(255),
                            Industry_code_ANZSIC06 varchar(255)
                        )
                    END
            ";
            using (var connection = new SqlConnection(Constants.connectionString))
            {
                connection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandText = sql;
                cmd.Connection = connection;
                cmd.ExecuteNonQuery();
            }
        }
    }
}
