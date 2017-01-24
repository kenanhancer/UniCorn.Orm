using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteInsertTests
    {
        UniOrm northwindSqlite;

        public SqliteInsertTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }

        [Fact]
        public void SimpleInsertQuery_Test1()
        {
            object result = northwindSqlite.ExecuteScalar("INSERT INTO Products (ProductName, UnitPrice) VALUES ('Pen', 13.5);SELECT LAST_INSERT_ROWID()");
        }

        [Fact]
        public void SimpleInsertQueryWithArgs_Test2()
        {
            object result = northwindSqlite.ExecuteScalar("INSERT INTO Products (ProductName, UnitPrice) VALUES (@ProductName, @UnitPrice);SELECT LAST_INSERT_ROWID()", args: new { ProductName = "Pen", UnitPrice = 13.5 });
        }

        [Fact]
        public void InsertQueryWithArgs_Test3()
        {
            object result = northwindSqlite.Insert(table: "Products",
                                                    pkField: "ProductID",
                                                    args: new { ProductID = 0, ProductName = "Pen", SupplierID = 123 });
        }

        [Fact]
        public void BulkInsert_Test4()
        {
            object result = northwindSqlite.Insert(table: "Products", 
                                                    pkField: "ProductID", 
                                                    args: new object[]
                                                     {
                                                         new { ProductID = 0, ProductName = "Pen" },
                                                         new { ProductID = 0, ProductName = "Notebook" }
                                                     });
        }
    }
}