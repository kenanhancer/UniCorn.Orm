using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data.Common;
using Xunit;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteCallbackTests
    {
        UniOrm northwindSqlite;

        public SqliteCallbackTests()
        {
            northwindSqlite = new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance);

            northwindSqlite.DefaultOptions = new Options
            {
                EventListener = new Listener
                {
                    OnCallback = (Callback f) =>
                    {
                        Console.WriteLine($"Executed Query is {f.SqlQuery}");
                    },
                    OnCommandPreExecuting = (DbCommand com) =>
                    {
                        foreach (DbParameter prm in com.Parameters)
                        {
                            Console.WriteLine(prm.ParameterName);
                        }
                    }
                }
            };
        }

        [Fact]
        public void SimpleQuery_Test1()
        {
            Listener listener = new Listener
            {
                OnCallback = (Callback f) =>
                {
                    //decimal UserID = f.OutputParameters[0].UserID;

                    Console.WriteLine($"Executed Query is {f.SqlQuery}");
                },
                OnCommandPreExecuting = (DbCommand com) =>
                {
                    foreach (DbParameter prm in com.Parameters)
                    {
                        Console.WriteLine(prm.ParameterName);
                    }
                }
            };

            IEnumerable<dynamic> result1 = northwindSqlite.Query(sql: "SELECT * FROM Products", options: new Options { EventListener = listener });
        }
    }
}