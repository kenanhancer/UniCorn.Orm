using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace UniCorn.Orm.ConsoleUnitTest
{
    public class Program
    {
        static UniOrm northwindSqlite;

        public static void Main(string[] args)
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);

            Test1();

            SqliteClassicTest();


        }

        static void Test1()
        {
            IEnumerable<Product> result12 = northwindSqlite.Query<Product>("SELECT * FROM Products");

            IEnumerable<Product> result22 = northwindSqlite.Query<Product>(table: "Products");

            IEnumerable<Product> result32 = northwindSqlite.Query<Product>(table: "Products", columns: "ProductID, ProductName, UnitPrice");

            IEnumerable<dynamic> result222 = northwindSqlite.Query(table: "Products");

            IEnumerable<dynamic> result323 = northwindSqlite.Query(table: "Products", columns: "ProductID, ProductName, UnitPrice");


            object result111 = northwindSqlite.Insert(table: "Products",
                                                    pkField: "ProductID",
                                                    args: new object[]
                                                     {
                                                         new { ProductID = 0, ProductName = "Pen" },
                                                         new { ProductID = 0, ProductName = "Notebook" }
                                                     });


            //QUERY
            {
                IEnumerable<dynamic> result1 = northwindSqlite.Query("SELECT * FROM Products WHERE ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                foreach (dynamic item in result1)
                {
                    Console.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(item));
                }

                IEnumerable<dynamic> result2 = northwindSqlite.Query("SELECT * FROM Products WHERE ProductID IN (@first, @second)", args: new { first = 1, second = 2 });

                IEnumerable<dynamic> result3 = northwindSqlite.Query("SELECT * FROM Products WHERE ProductID IN @ProductID", args: new { ProductID = new object[] { 1, 2 } });

                {
                    var args = new Dictionary<string, object> { { "first", 1 }, { "second", 2 } };

                    IEnumerable<dynamic> result = northwindSqlite.Query("SELECT * FROM Products WHERE ProductID IN (@first, @second)", args: args);
                }

                {
                    var args = new Dictionary<string, object> { { "ProductID", new object[] { 1, 2 } } };

                    IEnumerable<dynamic> result = northwindSqlite.Query("SELECT * FROM Products WHERE ProductID IN @ProductID", args: args);
                }

                {
                    dynamic args = new ExpandoObject();
                    args.ProductID = new object[] { 1, 2 };

                    IEnumerable<dynamic> result = northwindSqlite.Query("SELECT * FROM Products WHERE ProductID IN @ProductID", args: args);
                }

                string result5 = northwindSqlite.GeneratePocoCode("SELECT * FROM Products", "Products");

                IEnumerable<Product> result4 = northwindSqlite.Query<Product>("SELECT * FROM Products WHERE ProductID IN @ProductID", args: new { ProductID = new object[] { 1, 2 } });
            }

            //COUNT
            {
                int result1 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products");

                int result2 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                int result3 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN (@first, @second)", args: new { first = 1, second = 2 });

                int result4 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN @ProductID", args: new { ProductID = new object[] { 1, 2 } });

                {
                    var args = new Dictionary<string, object> { { "first", 1 }, { "second", 2 } };

                    int result = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN (@first, @second)", args: args);
                }

                {
                    var args = new Dictionary<string, object> { { "ProductID", new object[] { 1, 2 } } };

                    int result = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN @ProductID", args: args);
                }

                {
                    dynamic args = new ExpandoObject();
                    args.ProductID = new object[] { 1, 2 };

                    int result = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN @ProductID", args: args);
                }

            }

            //EXISTS
            {
                bool result1 = northwindSqlite.ExecuteScalar<bool>("SELECT * FROM Products");

                bool result2 = northwindSqlite.ExecuteScalar<bool>("SELECT * FROM Products WHERE ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                bool result3 = northwindSqlite.Exists(table: "Products", where: "ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                bool result4 = northwindSqlite.Exists(table: "Products", where: "ProductID IN @ProductID", args: new { ProductID = new object[] { 1, 2 } });
            }

            //SUM
            {
                int result1 = northwindSqlite.ExecuteScalar<int>("SELECT SUM(UnitPrice) FROM Products");

                int result2 = northwindSqlite.ExecuteScalar<int>("SELECT SUM(UnitPrice) FROM Products WHERE ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                object result3 = northwindSqlite.Sum(table: "Products", columns: "UnitPrice", where: "ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                object result4 = northwindSqlite.Sum(table: "Products", columns: "UnitPrice", where: "ProductID IN @ProductID", args: new { ProductID = new object[] { 1, 2 } });
            }

            //MAX
            {
                int result1 = northwindSqlite.ExecuteScalar<int>("SELECT MAX(UnitPrice) FROM Products");

                int result2 = northwindSqlite.ExecuteScalar<int>("SELECT MAX(UnitPrice) FROM Products WHERE ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                object result3 = northwindSqlite.Max(table: "Products", columns: "UnitPrice", where: "ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                object result4 = northwindSqlite.Max(table: "Products", columns: "UnitPrice", where: "ProductID IN @ProductID", args: new { ProductID = new object[] { 1, 2 } });
            }

            //MIN
            {
                int result1 = northwindSqlite.ExecuteScalar<int>("SELECT MIN(UnitPrice) FROM Products");

                int result2 = northwindSqlite.ExecuteScalar<int>("SELECT MIN(UnitPrice) FROM Products WHERE ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                object result3 = northwindSqlite.Min(table: "Products", columns: "UnitPrice", where: "ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                object result4 = northwindSqlite.Min(table: "Products", columns: "UnitPrice", where: "ProductID IN @ProductID", args: new { ProductID = new object[] { 1, 2 } });
            }

            //AVG
            {
                double result1 = northwindSqlite.ExecuteScalar<double>("SELECT AVG(UnitPrice) FROM Products");

                double result2 = northwindSqlite.ExecuteScalar<double>("SELECT AVG(UnitPrice) FROM Products WHERE ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                object result3 = northwindSqlite.Avg(table: "Products", columns: "UnitPrice", where: "ProductID IN (@0, @1)", args: new object[] { 1, 2 });

                object result4 = northwindSqlite.Avg(table: "Products", columns: "UnitPrice", where: "ProductID IN @ProductID", args: new { ProductID = new object[] { 1, 2 } });

            }

            //INSERT
            {
                object result = northwindSqlite.ExecuteNonQuery("INSERT INTO Products (ProductName) VALUES ('yenikalem')");
            }


            var a1 = new Product { ProductID = 1, ProductName = "Kalem" };

        }

        static void SqliteClassicTest()
        {
            Stopwatch sw = new Stopwatch();
            DbConnection con = northwindSqlite.NewConnection();
            DbCommand com = northwindSqlite.NewCommand(con, "SELECT UnitPrice, ProductID, ProductName FROM Products");

            con.Open();

            DbDataReader reader = com.ExecuteReader();

            sw.Start();

            List<Product> result = ReaderMapToEntity<Product>(reader);

            sw.Stop();

            con.Close();

            Console.WriteLine(sw.ElapsedMilliseconds);

            Console.ReadLine();
        }

        public static List<T> ReaderMapToEntity<T>(DbDataReader reader) where T : new()
        {
            List<T> result = new List<T>();

            T newItem;
            PropertyInfo[] properties = typeof(T).GetProperties();
            int fieldCount = reader.FieldCount;
            int ordinal;
            string fieldName;
            Type fieldType;
            MethodInfo propertySetMethod = null;
            MethodInfo readerGetValueMethod = null;
            PropertyInfo pi;
            Type readerType = typeof(DbDataReader);
            ReaderField readerField;
            object readerItemValue;
            List<ReaderField> fieldList = new List<ReaderField>();

            for (int i = 0; i < fieldCount; i++)
            {
                fieldName = reader.GetName(i);
                ordinal = reader.GetOrdinal(fieldName);
                fieldType = reader.GetFieldType(ordinal);

                pi = properties.FirstOrDefault(f => f.Name == fieldName);

                if (pi == null) continue;

                propertySetMethod = pi.GetSetMethod();

                readerGetValueMethod = readerType.GetMethod(string.Format("Get{0}", fieldType.Name));

                readerField = new ReaderField { Name = fieldName, Ordinal = ordinal, Property = pi, DataReaderGetValueMethod = readerGetValueMethod, PropertyFieldSetMethod = propertySetMethod };

                fieldList.Add(readerField);
            }

            while (reader.Read())
            {
                newItem = new T();

                for (int i = 0; i < fieldList.Count; i++)
                {
                    readerField = fieldList[i];

                    ordinal = readerField.Ordinal;

                    if (!reader.IsDBNull(ordinal))
                    {
                        //readerItemValue = reader.GetValue(ordinal);
                        //readerField.Property.SetValue(newItem, readerItemValue);

                        readerItemValue = readerField.DataReaderGetValueMethod.Invoke(reader, new object[] { ordinal });
                        readerField.PropertyFieldSetMethod.Invoke(newItem, new object[] { readerItemValue });
                    }
                }

                result.Add(newItem);
            }

            return result;
        }

    }
}

public struct ReaderField
{
    public int Ordinal { get; set; }
    public string Name { get; set; }
    public PropertyInfo Property { get; set; }
    public MethodInfo PropertyFieldSetMethod { get; set; }
    public MethodInfo DataReaderGetValueMethod { get; set; }
}