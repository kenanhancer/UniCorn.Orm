﻿using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;
using UniCorn.Core;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteAvgTests
    {
        UniOrm northwindSqlite;

        public SqliteAvgTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }

        [Fact]
        public void Avg_Test1()
        {
            double result1 = northwindSqlite.ExecuteScalar<double>("SELECT AVG(UnitPrice) FROM Products");

            object result2 = northwindSqlite.Avg(table: "Products", columns: "UnitPrice");

            Assert.True(result1 > 0);
            Assert.True(result2.To<double>() > 0);
        }

        [Fact]
        public void AvgWithArrayArgs_Test2()
        {
            var args = new object[] { 1, 2 };

            double result1 = northwindSqlite.ExecuteScalar<double>("SELECT AVG(UnitPrice) FROM Products WHERE ProductID IN (@0, @1)", args: args);

            object result2 = northwindSqlite.Avg(table: "Products", columns: "UnitPrice", where: "ProductID IN (@0, @1)", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2.To<double>() > 0);
        }

        [Fact]
        public void AvgWithAnonymousArgs_Test3()
        {
            var args = new { first = 1, second = 2 };

            double result1 = northwindSqlite.ExecuteScalar<double>("SELECT AVG(UnitPrice) FROM Products WHERE ProductID IN (@first, @second)", args: args);

            object result2 = northwindSqlite.Avg(table: "Products", columns: "UnitPrice", where: "ProductID IN (@first, @second)", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2.To<double>() > 0);
        }

        [Fact]
        public void AvgWithDictArgs_Test4()
        {
            var args = new Dictionary<string, object> { { "first", 1 }, { "second", 2 } };

            double result1 = northwindSqlite.ExecuteScalar<double>("SELECT AVG(UnitPrice) FROM Products WHERE ProductID IN (@first, @second)", args: args);

            object result2 = northwindSqlite.Avg(table: "Products", columns: "UnitPrice", where: "ProductID IN (@first, @second)", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2.To<double>() > 0);
        }

        [Fact]
        public void AvgWithAnonymousArgs_Test5()
        {
            var args = new { ProductID = new object[] { 1, 2 } };

            double result1 = northwindSqlite.ExecuteScalar<int>("SELECT AVG(UnitPrice) FROM Products WHERE ProductID IN @ProductID", args: args);

            object result2 = northwindSqlite.Avg(table: "Products", columns: "UnitPrice", where: "ProductID IN @ProductID", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2.To<double>() > 0);
        }

        [Fact]
        public void AvgWithDictArgs_Test6()
        {
            dynamic args = new ExpandoObject();
            args.ProductID = new object[] { 1, 2 };

            double result1 = northwindSqlite.ExecuteScalar<double>("SELECT AVG(UnitPrice) FROM Products WHERE ProductID IN @ProductID", args: args);

            object result2 = northwindSqlite.Avg(table: "Products", columns: "UnitPrice", where: "ProductID IN @ProductID", args: (object)args);

            Assert.True(result1 > 0);
            Assert.True(result2.To<double>() > 0);
        }

        [Fact]
        public void AvgWithDictArgs_Test7()
        {
            var args = new Dictionary<string, object> { { "ProductID", new object[] { 1, 2 } } };

            double result1 = northwindSqlite.ExecuteScalar<double>("SELECT AVG(UnitPrice) FROM Products WHERE ProductID IN @ProductID", args: args);

            object result2 = northwindSqlite.Avg(table: "Products", columns: "UnitPrice", where: "ProductID IN @ProductID", args: args);

            Assert.True(result1 > 0);
            Assert.True(result2.To<double>() > 0);
        }
    }
}