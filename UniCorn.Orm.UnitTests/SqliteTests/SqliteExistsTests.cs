using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteExistsTests
    {
        UniOrm northwindSqlite;

        public SqliteExistsTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }

        [Fact]
        public void Exists_Test1()
        {
            bool result1 = northwindSqlite.ExecuteScalar<bool>("SELECT * FROM Products");

            bool result2 = northwindSqlite.Exists(table: "Products");

            Assert.True(result1);
            Assert.True(result2);
        }

        [Fact]
        public void ExistsWithArrayArgs_Test2()
        {
            var args = new object[] { 1, 2 };

            bool result1 = northwindSqlite.ExecuteScalar<bool>("SELECT * FROM Products WHERE ProductID IN (@0, @1)", args: args);

            bool result2 = northwindSqlite.Exists(table: "Products", where: "ProductID IN (@0, @1)", args: args);

            Assert.True(result1);
            Assert.True(result2);
        }

        [Fact]
        public void ExistsWithAnonymousArgs_Test3()
        {
            var args = new { first = 1, second = 2 };

            bool result1 = northwindSqlite.ExecuteScalar<bool>("SELECT * FROM Products WHERE ProductID IN (@first, @second)", args: args);

            bool result2 = northwindSqlite.Exists(table: "Products", where: "ProductID IN (@first, @second)", args: args);

            Assert.True(result1);
            Assert.True(result2);
        }

        [Fact]
        public void ExistsWithDictArgs_Test4()
        {
            var args = new Dictionary<string, object> { { "first", 1 }, { "second", 2 } };

            bool result1 = northwindSqlite.ExecuteScalar<bool>("SELECT * FROM Products WHERE ProductID IN (@first, @second)", args: args);

            bool result2 = northwindSqlite.Exists(table: "Products", where: "ProductID IN (@first, @second)", args: args);

            Assert.True(result1);
            Assert.True(result2);
        }

        [Fact]
        public void ExistsWithAnonymousArgs_Test5()
        {
            var args = new { ProductID = new object[] { 1, 2 } };

            bool result1 = northwindSqlite.ExecuteScalar<bool>("SELECT * FROM Products WHERE ProductID IN @ProductID", args: args);

            bool result2 = northwindSqlite.Exists(table: "Products", where: "ProductID IN @ProductID", args: args);

            Assert.True(result1);
            Assert.True(result2);
        }

        [Fact]
        public void ExistsWithDictArgs_Test6()
        {
            dynamic args = new ExpandoObject();
            args.ProductID = new object[] { 1, 2 };

            bool result1 = northwindSqlite.ExecuteScalar<bool>("SELECT * FROM Products WHERE ProductID IN @ProductID", args: args);

            bool result2 = northwindSqlite.Exists(table: "Products", where: "ProductID IN @ProductID", args: (object)args);

            Assert.True(result1);
            Assert.True(result2);
        }

        [Fact]
        public void ExistsWithDictArgs_Test7()
        {
            var args = new Dictionary<string, object> { { "ProductID", new object[] { 1, 2 } } };

            bool result1 = northwindSqlite.ExecuteScalar<bool>("SELECT * FROM Products WHERE ProductID IN @ProductID", args: args);

            bool result2 = northwindSqlite.Exists(table: "Products", where: "ProductID IN @ProductID", args: args);

            Assert.True(result1);
            Assert.True(result2);
        }
    }
}