using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Reflection;
using UniCorn.Core;
using Microsoft.Extensions.DependencyModel;
using System.Linq;
using System.Data.Common;
using System.IO;
using System.Runtime.Loader;

namespace UniCorn.Orm.Console
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Dictionary<string, string> argsDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            string arg;
            for (int i = 0; i < args.Length; i++)
            {
                arg = args[i];

                if (arg.StartsWith("-"))
                    argsDict.Add(arg.Substring(1), args[++i]);
            }

            string conName;
            if(!argsDict.TryGetValue("conname", out conName) || String.IsNullOrEmpty(conName))
                throw new ArgumentNullException("ConName");

            string sql;
            if (!argsDict.TryGetValue("sql", out sql) || String.IsNullOrEmpty(sql))
                throw new ArgumentNullException("Sql");

            string formattingArg;
            argsDict.TryGetValue("formatting", out formattingArg);

            Formatting formatting = formattingArg.To<Formatting>();

            //Assembly assembly = Assembly.Load(new AssemblyName("Microsoft.Data.Sqlite"));

            //Type sqliteFactory = assembly.GetType("Microsoft.Data.Sqlite.SqliteFactory");

            //FieldInfo instanceOfDbProviderFactoryFi = sqliteFactory.GetField("Instance");

            //DbProviderFactory instanceOfDbProviderFactory = instanceOfDbProviderFactoryFi.GetValue(null) as DbProviderFactory;

            //MySql.Data.MySqlClient.MySqlClientFactory


            UniOrm db = new UniOrm($"ConnectionStrings:{conName}", null);

            IEnumerable<dynamic> result = db.Query(sql);

            string json = JsonConvert.SerializeObject(result, formatting);

            System.Console.WriteLine(json);
        }
    }
}