using Microsoft.Data.Sqlite;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Xunit;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteQueryTests
    {
        UniOrm northwindSqlite;

        public SqliteQueryTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }

        [Fact]
        public void GeneratePocoCodeFromQuery()
        {
            string productPoco = northwindSqlite.GeneratePocoCode("SELECT * FROM Products", "Product");

            Assert.False(String.IsNullOrEmpty(productPoco));
        }

        [Fact]
        public void SimpleQuery_Test1()
        {
            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products");

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products");

            IEnumerable<dynamic> result3 = northwindSqlite.Query(table: "Products", columns: "ProductID, ProductName");

            Assert.True(result1.Count() > 0);
            Assert.True(result2.Count() > 0);
            Assert.True(result3.Count() > 0);


            var firstItem = result3.First() as IDictionary<string, object>;

            Assert.True(firstItem.Keys.Count == 2);
        }

        [Fact]
        public void QueryWithStronglyType_Test2()
        {
            IEnumerable<Product> result1 = northwindSqlite.Query<Product>(sql: "SELECT * FROM Products");

            IEnumerable<Product> result2 = northwindSqlite.Query<Product>(table: "Products");

            IEnumerable<Product> result3 = northwindSqlite.Query<Product>(table: "Products", columns: "ProductID, ProductName");



            IEnumerable<object> result4 = northwindSqlite.ExecuteReader(typeof(Product), "SELECT * FROM Products");

            Assert.True(result1.Count() > 0);
            Assert.True(result2.Count() > 0);
            Assert.True(result3.Count() > 0);
            Assert.True(result4.Count() > 0);
        }

        [Fact]
        public void QueryWithArrayArgs_Test3()
        {
            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products WHERE ProductID IN (@0, @1)", args: new object[] { 1, 2 });

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products", where: "ProductID IN (@0, @1)", args: new object[] { 1, 2 });

            Assert.True(result1.Count() > 0);
            Assert.True(result2.Count() > 0);
        }

        [Fact]
        public void QueryWithAnonymousArgs_Test4()
        {
            var args = new { first = 1, second = 2 };

            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products WHERE ProductID IN (@first, @second)", args: args);

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products", where: "ProductID IN (@first, @second)", args: args);

            Assert.True(result1.Count() > 0);
            Assert.True(result2.Count() > 0);
        }

        [Fact]
        public void QueryWithAnonymousArgs_Test5()
        {
            var args = new { ProductID = new object[] { 1, 2 } };

            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products WHERE ProductID IN @ProductID", args: args);

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products", where: "ProductID IN @ProductID", args: args);

            Assert.True(result1.Count() > 0);
            Assert.True(result2.Count() > 0);
        }

        [Fact]
        public void QueryWithDictArgs_Test6()
        {
            var args = new Dictionary<string, object> { { "first", 1 }, { "second", 2 } };

            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products WHERE ProductID IN (@first, @second)", args: args);

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products", where: "ProductID IN (@first, @second)", args: args);

            Assert.True(result1.Count() > 0);
            Assert.True(result2.Count() > 0);
        }

        [Fact]
        public void QueryWithDictArgs_Test7()
        {
            var args = new Dictionary<string, object> { { "ProductID", new object[] { 1, 2 } } };

            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products WHERE ProductID IN @ProductID", args: args);

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products", where: "ProductID IN @ProductID", args: args);

            Assert.True(result1.Count() > 0);
            Assert.True(result2.Count() > 0);
        }

        [Fact]
        public void QueryWithExpandoObjectArgs_Test8()
        {
            dynamic args = new ExpandoObject();
            args.ProductID = new object[] { 1, 2 };

            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products WHERE ProductID IN @ProductID", args: args);

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products", where: "ProductID IN @ProductID", args: (object)args);

            Assert.True(result1.Count() > 0);
        }
    }
}