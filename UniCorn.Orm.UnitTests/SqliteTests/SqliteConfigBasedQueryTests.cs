using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using Xunit;
using Newtonsoft.Json.Linq;

namespace UniCorn.Orm.UnitTests.SqliteTests
{
    public class SqliteConfigBasedQueryTests
    {
        UniOrm northwindSqlite;

        public SqliteConfigBasedQueryTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }

        [Fact]
        public void ConfigBasedQuery_Test()
        {
            var options = new Options
            {
                EventListener = new Listener
                {
                    OnCallback = (Callback f) =>
                    {
                        Console.WriteLine(f.SqlQuery);
                    }
                }
            };

            var criteria = new SqlEntity
            {
                OperationName = "Query",
                Table = "Products",
                Columns = "ProductID, ProductName, UnitPrice",
                Where = "ProductID IN @ProductID",
                Args = new { ProductID = new[] { 1, 2 } },
                Options = options
            };

            //Generated Sql: SELECT ListPrice FROM [Production].[Product] WHERE Color in (@Color0,@Color1)
            IEnumerable<dynamic> result = northwindSqlite.Execute(criteria);

            Assert.True(result.Count() > 0);
        }

        [Fact]
        public void ConfigBasedQuery_WithJson_Test()
        {
            var options = new Options
            {
                EventListener = new Listener
                {
                    OnCallback = (Callback f) =>
                    {
                        Console.WriteLine(f.SqlQuery);
                    }
                }
            };

            string json = @"{
              'OperationName': 'Query',
              'Table': 'Products',
              'Columns': 'ProductID, ProductName, UnitPrice',
              'Where': 'ProductID IN @ProductID',
              'Args': { 'ProductID': [1, 2] }
            }";

            SqlEntity criteria = Newtonsoft.Json.JsonConvert.DeserializeObject<SqlEntity>(json);
            criteria.Options = options;
            
            //Generated Sql: SELECT ListPrice FROM [Production].[Product] WHERE Color in (@Color0,@Color1)
            IEnumerable<dynamic> result = northwindSqlite.Execute(criteria);

            Assert.True(result.Count() > 0);
        }
    }
}