using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Xunit;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteLimitTests
    {
        UniOrm northwindSqlite;

        public SqliteLimitTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }

        [Fact]
        public void SimpleQueryWithLimit_Test1()
        {
            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products ORDER BY ProductID DESC LIMIT 5");

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products", orderby: "ProductID DESC", limit: 5);

            IEnumerable<dynamic> result3 = northwindSqlite.Query(table: "Products", columns: "ProductID, ProductName, UnitPrice", orderby: "ProductID DESC", limit: 5);

            Assert.True(result1.Count() == 5);
            Assert.True(result2.Count() == 5);
            Assert.True(result3.Count() == 5);


            var firstItem = result3.First() as IDictionary<string, object>;

            Assert.True(firstItem.Keys.Count == 3);
        }

        [Fact]
        public void SimpleQueryWithLimit_Test2()
        {
            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products WHERE UnitPrice > 0 ORDER BY ProductID DESC LIMIT 5");

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products", where: "UnitPrice > 0", orderby: "ProductID DESC", limit: 5);

            IEnumerable<dynamic> result3 = northwindSqlite.Query(table: "Products", columns: "ProductID, ProductName, UnitPrice", where: "UnitPrice > 0", orderby: "ProductID DESC", limit: 5);

            Assert.True(result1.Count() == 5);
            Assert.True(result2.Count() == 5);
            Assert.True(result3.Count() == 5);


            var firstItem = result3.First() as IDictionary<string, object>;

            Assert.True(firstItem.Keys.Count == 3);
        }
    }
}