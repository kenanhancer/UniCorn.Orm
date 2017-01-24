using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using UniCorn.IoC;
using Xunit;

namespace UniCorn.Orm.UnitTests
{
    public class SqliteIoCContainerTests
    {
        UniIoC container;

        public SqliteIoCContainerTests()
        {
            container = new UniIoC();

            container.Register(ServiceCriteria.For<IUniOrm>().ImplementedBy<UniOrm>().Named("northwindSqlite").LifeCycle(LifeCycleEnum.Singleton).OnInstanceCreating(f => new UniOrm("ConnectionStrings:NorthwindSqlite", SqliteFactory.Instance)).OnIntercepting(Intercepting));
        }

        void Intercepting(IInvocation invocation)
        {
            invocation.Proceed();
        }

        [Fact]
        public void TestMethod1()
        {
            IUniOrm northwindSqlite = container.Resolve<IUniOrm>("northwindSqlite");

            IUniOrm northwindSqlite2 = container.Resolve<IUniOrm>("northwindSqlite");

            string productPoco = northwindSqlite.GeneratePocoCode("SELECT * FROM Products", "Product");

            Assert.False(String.IsNullOrEmpty(productPoco));

            Assert.Equal(northwindSqlite, northwindSqlite2);
        }
    }
}