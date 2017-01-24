using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Linq;
using UniCorn.Core;
using Xunit;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteUpdateTests
    {
        UniOrm northwindSqlite;

        public SqliteUpdateTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }

        [Fact]
        public void SimpleUpdateQueryWithArgs_Test1()
        {
            object insertRowID = northwindSqlite.ExecuteScalar("INSERT INTO Products (ProductName, UnitPrice) VALUES (@ProductName, @UnitPrice);SELECT LAST_INSERT_ROWID()", args: new { ProductName = "Pen", UnitPrice = 13.5 });

            int result = northwindSqlite.ExecuteNonQuery("UPDATE Products SET ProductName='Pen', UnitPrice = 45.878 WHERE ProductID = @ProductID", args: new { ProductID = insertRowID });
        }

        [Fact]
        public void SimpleUpdateQueryWithArgs_Test2()
        {
            object insertRowID = northwindSqlite.ExecuteScalar("INSERT INTO Products (ProductName, UnitPrice) VALUES (@ProductName, @UnitPrice);SELECT LAST_INSERT_ROWID()", args: new { ProductName = "Pen", UnitPrice = 13.5 });

            object result = northwindSqlite.Update(table: "Products",
                                                   columns: "ProductName, SupplierID",
                                                   where: "ProductID = @ProductID",
                                                   args: new { ProductID = insertRowID, ProductName = "Pencil", SupplierID = 123, UnitPrice = 33 });
        }

        [Fact]
        public void BulkUpdateTest()
        {
            IEnumerable<Product> productList = northwindSqlite.Query<Product>(sql: "SELECT * FROM Products ORDER BY ProductID DESC LIMIT 5");

            foreach (var product in productList)
            {
                product.ProductName = "TEST PRODUCT 2";
            }

            var result2 = northwindSqlite.Update(table: "Products",
                                                 columns: "ProductName",
                                                 where: "ProductID=@ProductID AND UnitPrice>0",
                                                 args: productList.ToArray());
        }
    }
}