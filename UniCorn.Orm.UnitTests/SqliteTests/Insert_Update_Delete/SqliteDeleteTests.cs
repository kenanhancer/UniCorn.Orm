using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteDeleteTests
    {
        UniOrm northwindSqlite;

        public SqliteDeleteTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);
        }
    }
}