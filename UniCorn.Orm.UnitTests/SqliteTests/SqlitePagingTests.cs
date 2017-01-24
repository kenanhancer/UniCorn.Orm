using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Xunit;

namespace UniCorn.Orm.UnitTests.SqliteTests
{
    public class SqlitePagingTests
    {
        UniOrm northwindSqlite;

        public SqlitePagingTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }

        [Fact]
        public void SimpleQueryWithLimit_Test1()
        {
            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products LIMIT 0,50");

            IEnumerable<dynamic> result2 = northwindSqlite.Query(table: "Products", orderby: "ProductID DESC", pageNo: 1, pageSize: 50);

            IEnumerable<dynamic> result3 = northwindSqlite.Query(table: "Products", columns: "ProductID, ProductName, UnitPrice", orderby: "ProductID DESC", pageNo: 1, pageSize: 50);

            Assert.True(result1.Count() == 50);
            Assert.True(result2.Count() == 50);
            Assert.True(result3.Count() == 50);


            var firstItem = result3.First() as IDictionary<string, object>;

            Assert.True(firstItem.Keys.Count == 3);
        }
    }
}