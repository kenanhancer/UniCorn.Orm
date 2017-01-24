using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteCountTests
    {
        UniOrm northwindSqlite;

        public SqliteCountTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }

        [Fact]
        public void Count_Test1()
        {
            int result1 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products");

            int result2 = northwindSqlite.Count(table: "Products");

            Assert.True(result1 > 0);
            Assert.True(result2 > 0);
        }

        [Fact]
        public void CountWithArrayArgs_Test2()
        {
            var args = new object[] { 1, 2 };

            int result1 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN (@0, @1)", args: args);

            int result2 = northwindSqlite.Count(table: "Products", where: "ProductID IN (@0, @1)", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2 > 0);
        }

        [Fact]
        public void CountWithAnonymousArgs_Test3()
        {
            var args = new { first = 1, second = 2 };

            int result1 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN (@first, @second)", args: args);

            int result2 = northwindSqlite.Count(table: "Products", where: "ProductID IN (@first, @second)", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2 > 0);
        }

        [Fact]
        public void CountWithDictArgs_Test4()
        {
            var args = new Dictionary<string, object> { { "first", 1 }, { "second", 2 } };

            int result1 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN (@first, @second)", args: args);

            int result2 = northwindSqlite.Count(table: "Products", where: "ProductID IN (@first, @second)", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2 > 0);
        }

        [Fact]
        public void CountWithAnonymousArgs_Test5()
        {
            var args = new { ProductID = new object[] { 1, 2 } };

            int result1 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN @ProductID", args: args);

            int result2 = northwindSqlite.Count(table: "Products", where: "ProductID IN @ProductID", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2 > 0);
        }

        [Fact]
        public void CountWithDictArgs_Test6()
        {
            dynamic args = new ExpandoObject();
            args.ProductID = new object[] { 1, 2 };

            int result1 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN @ProductID", args: args);

            int result2 = northwindSqlite.Count(table: "Products", where: "ProductID IN @ProductID", args: (object)args);

            Assert.True(result1 > 0);
            Assert.True(result2 > 0);
        }

        [Fact]
        public void CountWithDictArgs_Test7()
        {
            var args = new Dictionary<string, object> { { "ProductID", new object[] { 1, 2 } } };

            int result1 = northwindSqlite.ExecuteScalar<int>("SELECT COUNT(*) FROM Products WHERE ProductID IN @ProductID", args: args);

            int result2 = northwindSqlite.Count(table: "Products", where: "ProductID IN @ProductID", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2 > 0);
        }
    }
}