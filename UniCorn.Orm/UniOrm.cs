using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using UniCorn.Core;
namespace UniCorn.Orm
{
    #region Entities
    public enum DatabaseType { SQLServer, SqlServerCE, MySQL, Oracle, SQLite, PostgreSQL, OleDB }
    public class Listener
    {
        public Action<DbCommand> OnCommandPreExecuting { get; set; }
        //public Func<object, object> OnConvertingResult { get; set; }
        public Action<Callback> OnCallback { get; set; }
    }
    public class Options
    {
        public bool FieldNameLower { get; set; }
        public Listener EventListener { get; set; } = new Listener();
        public DbTransaction Transaction { get; set; }
    }
    public class Callback
    {
        public string SqlQuery { get; set; }
        public dynamic OutputParameters { get; set; }
        public TimeSpan ElapsedTime { get; set; }
        public int RowCount { get; set; }
        //public object Result { get; set; }
    }
    public class SqlEntity
    {
        DatabaseType databaseType;
        string columns;
        string where;
        string parameterPrefix;
        string[] columnArray;
        string[] whereParameterArray;
        object args;
        public string OperationName { get; set; }
        public string Package { get; set; }
        public string Schema { get; set; }
        public string Table { get; set; }
        public string Columns
        {
            get { return columns; }
            set
            {
                columns = value;
                columnArray = columns?.Split(new char[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries) ?? new string[0];
            }
        }
        public string[] ColumnArray => columnArray;
        public string Where
        {
            get { return where; }
            set
            {
                where = value;

                SetWhereParameterArray();
            }
        }
        public string[] WhereParameterArray => whereParameterArray;
        private void SetWhereParameterArray()
        {
            if (String.IsNullOrEmpty(ParameterPrefix) || String.IsNullOrEmpty(Where)) return;

            Regex regex = new Regex(string.Format("(?<parameter>{0}[^{0},;) ]+)", ParameterPrefix));
            MatchCollection parameterMatchCollection = regex.Matches(Where);
            whereParameterArray = parameterMatchCollection.Cast<Match>().Select(f => f.Groups["parameter"].Value.Substring(1)).Distinct().ToArray();
        }
        public string OrderBy { get; set; }
        public string GroupBy { get; set; }
        public string Having { get; set; }
        public string Fn { get; set; }
        public string Sp { get; set; }
        public string Sql { get; set; }
        public string PKField { get; set; }
        public string Sequence { get; set; }
        public string RowNumberColumn { get; set; }
        //public string GeneratedSql { get; set; }
        public long Limit { get; set; }
        public long PageSize { get; set; }
        public long PageNo { get; set; }
        public string ParameterPrefix
        {
            get { return parameterPrefix; }
            private set
            {
                parameterPrefix = value;

                SetWhereParameterArray();
            }
        }
        public string ParameterSuffix { get; set; }
        public DatabaseType DatabaseType
        {
            get { return databaseType; }
            set
            {
                databaseType = value;
                ParameterPrefix = databaseType.GetDatabaseParameterPrefix();
            }
        }
        public object Args
        {
            get { return args; }
            set
            {
                if (value is JObject)
                {
                    JObject argsJObj = value as JObject;
                    args = argsJObj.ToDictionary();
                }
                else if (value is JArray)
                {
                    JArray argsJArray = value as JArray;
                    args = argsJArray.ToArray();
                }
                else
                {
                    if (value != null && value is string)// && value.ToString().IndexOfAny(new char[] { '{', '[' }) > -1)
                    {
                        Args = Newtonsoft.Json.JsonConvert.DeserializeObject(value.ToString());
                    }
                    else
                        args = value;
                }
            }
        }
        public Options Options { get; set; }
        public SqlEntity()
        {
            Columns = "*";
        }
        public static string BuildSql(SqlEntity sqlEntity)
        {
            string operationName = sqlEntity.OperationName.ToLowerInvariant();
            string columnStatement = sqlEntity.Columns;
            string dbObjectName = $"{sqlEntity.Schema}.{sqlEntity.Table}".Trim('.');
            string whereStatement = sqlEntity.Where.GetValueIfNotNullorEmpty()?.Insert(0, "WHERE ");

            string sqlStatement = null;
            DatabaseType databaseType = sqlEntity.DatabaseType;
            string columns = sqlEntity.Columns;
            string pkField = sqlEntity.PKField;
            string parameterPrefix = sqlEntity.ParameterPrefix;
            string pkFieldWithPrefix = $"{parameterPrefix}{pkField}";
            string[] columnArray = sqlEntity.ColumnArray;

            if (operationName == "query" || operationName == "multiquery" || operationName == "exists" || operationName == "count" || operationName == "sum" || operationName == "max" || operationName == "min" || operationName == "avg")
            {
                #region SELECT
                string orderByStatement = sqlEntity.OrderBy.GetValueIfNotNullorEmpty()?.Insert(0, " ORDER BY ");
                string groupByStatement = sqlEntity.GroupBy.GetValueIfNotNullorEmpty()?.Insert(0, " GROUP BY ");
                string havingStatement = sqlEntity.Having.GetValueIfNotNullorEmpty()?.Insert(0, " HAVING ");
                string limitStatement = null;
                long limit = sqlEntity.Limit;

                if (String.IsNullOrEmpty(sqlEntity.Sql))
                {
                    if (String.IsNullOrEmpty(dbObjectName))
                        throw new ArgumentNullException("Database object name cannot be null.");

                    #region Columns
                    if (String.IsNullOrEmpty(columnStatement))
                        throw new ArgumentNullException("Column statement cannot be null.");
                    else
                    {
                        if (operationName == "count" || operationName == "sum" || operationName == "max" || operationName == "min" || operationName == "avg")
                            if (columnArray.Length > 1)
                                throw new Exception($"Columns cannot be more than one for {operationName}");
                            else if (columnStatement == "*" && (operationName == "sum" || operationName == "max" || operationName == "min" || operationName == "avg"))
                                throw new Exception($"Columns cannot be * for {operationName}");
                            else
                                columnStatement = $"{operationName.ToUpperInvariant()}({columnStatement}) {operationName}";
                    }
                    #endregion Columns

                    if (limit > 0)
                    {
                        columnStatement = databaseType == DatabaseType.SQLServer ? $"TOP {limit} {columns}" : columns;
                        if (databaseType == DatabaseType.MySQL || databaseType == DatabaseType.PostgreSQL || databaseType == DatabaseType.SQLite)
                            limitStatement = $" LIMIT {limit} ";
                    }

                    sqlStatement = $"SELECT {columnStatement} FROM {dbObjectName} {whereStatement}{groupByStatement}{havingStatement}{orderByStatement}{limitStatement}";

                    if (limit > 0 && databaseType == DatabaseType.Oracle)
                        sqlStatement = $"SELECT * FROM ({sqlStatement}) WHERE ROWNUM <= {limit}";

                    if (sqlEntity.PageSize > 0 || sqlEntity.PageNo > 0)
                    {
                        if (sqlEntity.PageNo <= 0) sqlEntity.PageNo = 1;
                        if (sqlEntity.PageSize <= 0) sqlEntity.PageSize = 10;
                        long pageStart = (sqlEntity.PageNo - 1) * sqlEntity.PageSize;
                        if (databaseType == DatabaseType.SQLServer)
                            sqlStatement = $"SELECT TOP {sqlEntity.PageSize} * FROM (SELECT ROW_NUMBER() OVER (ORDER BY {sqlEntity.RowNumberColumn}) AS RowNumber, * FROM ({sqlStatement}) as PagedTable) as PagedRecords WHERE RowNumber > {pageStart}";
                        else if (databaseType == DatabaseType.Oracle)
                            sqlStatement = $"SELECT * FROM (SELECT T1.*,ROWNUM ROWNUMBER FROM ({sqlStatement}) T1 WHERE ROWNUM <= {pageStart + sqlEntity.PageSize}) WHERE ROWNUMBER > {pageStart}";
                        else if (databaseType == DatabaseType.MySQL || databaseType == DatabaseType.SQLite)
                            sqlStatement = $"{sqlStatement} LIMIT {pageStart},{sqlEntity.PageSize}";
                    }

                    if (operationName == "exists")
                    {
                        string existSuffix = databaseType == DatabaseType.Oracle ? "FROM DUAL" : null;
                        sqlStatement = $"SELECT CASE WHEN EXISTS({sqlStatement}) THEN 1 ELSE 0 END as RESULT {existSuffix}";
                    }
                }
                else
                {
                    if (whereStatement == null && groupByStatement == null && havingStatement == null && orderByStatement == null && limitStatement == null)
                        sqlStatement = sqlEntity.Sql;
                    else
                        sqlStatement = $"SELECT * FROM ({sqlEntity.Sql}){whereStatement}{groupByStatement}{havingStatement}{orderByStatement}{limitStatement}";
                }
                #endregion SELECT
            }
            else if (operationName == "insert")
            {
                #region INSERT
                string sequence = sqlEntity.Sequence;
                string pkFieldToSequence = null;
                string newIDSql = null;

                ICollection argsCol = sqlEntity.Args as ICollection;

                if (argsCol != null)
                {
                    var insertList = new List<string>();
                    SqlEntity newSqlEntity = null;
                    int indexOfArgsCol = 0;
                    foreach (object argItem in argsCol)
                    {
                        newSqlEntity = sqlEntity.MemberwiseClone() as SqlEntity;
                        newSqlEntity.Args = argItem;
                        newSqlEntity.ParameterSuffix = indexOfArgsCol.ToString();
                        insertList.Add(SqlEntity.BuildSql(newSqlEntity));
                        indexOfArgsCol++;
                    }
                    sqlStatement = String.Join(";", insertList);

                    if (databaseType == DatabaseType.Oracle)
                        sqlStatement = $"BEGIN {sqlStatement} RETURNING 1 INTO {pkFieldWithPrefix};END;";
                    return sqlStatement.Trim();
                }

                if (databaseType == DatabaseType.Oracle)
                {
                    newIDSql = $"RETURNING {pkField} INTO {pkFieldWithPrefix}";

                    if (!String.IsNullOrEmpty(sequence))
                    {
                        pkField = "";
                        pkFieldToSequence = $"{sequence}.NEXTVAL";
                    }
                }
                else if (databaseType == DatabaseType.PostgreSQL)
                {
                    newIDSql = $"RETURNING {pkField}";

                    if (!String.IsNullOrEmpty(sequence))
                    {
                        pkField = "";
                        pkFieldToSequence = $"NEXTVAL('{sequence}')";
                    }

                }
                else if (databaseType == DatabaseType.SQLServer)
                    newIDSql = ";SELECT SCOPE_IDENTITY()";
                else if (databaseType == DatabaseType.MySQL)
                    newIDSql = ";SELECT LAST_INSERT_ID()";
                else if (databaseType == DatabaseType.SQLite)
                    newIDSql = ";SELECT LAST_INSERT_ROWID()";

                string insertColumns = sqlEntity.Args.ToColumnString(pkField);
                string insertParameters = sqlEntity.Args.ToParameterString(parameterPrefix, pkField, sqlEntity.ParameterSuffix).Replace(pkFieldWithPrefix, pkFieldToSequence);

                sqlStatement = $"INSERT INTO {dbObjectName} ({insertColumns}) VALUES ({insertParameters}) {newIDSql}";

                #endregion INSERT
            }
            else if (operationName == "update")
            {
                #region UPDATE

                ICollection argsCol = sqlEntity.Args as ICollection;

                if (argsCol != null)
                {
                    var updateList = new List<string>();
                    SqlEntity newSqlEntity = null;
                    int indexOfArgsCol = 0;
                    foreach (object argItem in argsCol)
                    {
                        newSqlEntity = sqlEntity.MemberwiseClone() as SqlEntity;
                        newSqlEntity.Args = argItem;
                        newSqlEntity.ParameterSuffix = indexOfArgsCol.ToString();
                        sqlStatement = SqlEntity.BuildSql(newSqlEntity);
                        updateList.Add(sqlStatement);
                        indexOfArgsCol++;
                    }
                    sqlStatement = String.Join(";", updateList);

                    if (databaseType == DatabaseType.Oracle)
                        sqlStatement = $"BEGIN {sqlStatement} RETURNING 1 INTO {pkFieldWithPrefix};END;";
                    return sqlStatement.Trim();
                }

                string parameterSuffix = sqlEntity.ParameterSuffix;
                string updateSetStatement = sqlEntity.Args.ToColumnAndParameterString(parameterPrefix, pkField, parameterSuffix, includedFields: columnArray);
                string parameterName;
                foreach (string prm in sqlEntity.WhereParameterArray)
                {
                    parameterName = parameterPrefix + prm;
                    whereStatement = whereStatement.Replace(parameterName, $"{parameterName}{parameterSuffix}");
                }


                sqlStatement = $"UPDATE {dbObjectName} SET {updateSetStatement} {whereStatement}";
                #endregion UPDATE
            }
            else if (operationName == "delete")
            {
                #region DELETE

                #endregion DELETE
            }

            return sqlStatement.Trim();
        }
    }
    public interface IUniOrm
    {
        string ConnectionString { get; }
        string ProviderName { get; }
        DatabaseType DatabaseType { get; }
        string ParameterPrefix { get; }
        Options DefaultOptions { get; set; }
        DbConnection NewConnection();
        DbCommand NewCommand(DbConnection con, string commandText, Options options = null, object args = null);
        DbParameter NewParameter(DbCommand com, string parameterName, object value, DbType dbType = System.Data.DbType.Object, ParameterDirection parameterDirection = ParameterDirection.Input, Options options = null);
        IEnumerable<dynamic> ExecuteReader(string commandText = "", Options options = null, object args = null);
        IEnumerable<object> ExecuteReader(Type type, string commandText = "", Options options = null, object args = null);
        IEnumerable<T> ExecuteReader<T>(string commandText = "", Options options = null, object args = null) where T : new();
        object ExecuteScalar(string commandText = "", Options options = null, object args = null);
        T ExecuteScalar<T>(string commandText = "", Options options = null, object args = null) where T : new();
        int ExecuteNonQuery(string commandText = "", Options options = null, object args = null);

        dynamic Execute(SqlEntity sqlEntity);
        IEnumerable<dynamic> Query(string sql = "", string schema = "", string table = "", string columns = "", string where = "", string orderby = "", string groupby = "", string having = "", int limit = 0, int pageSize = 0, int pageNo = 0, Options options = null, object args = null);
        IEnumerable<T> Query<T>(string sql = "", string schema = "", string table = "", string columns = "*", string where = "", string orderby = "", string groupby = "", string having = "", int limit = 0, int pageSize = 0, int pageNo = 0, Options options = null, object args = null) where T : new();
        object Avg(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null);
        object Max(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null);
        object Min(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null);
        object Sum(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null);
        int Count(string schema = "", string table = "", string where = "", Options options = null, object args = null);
        bool Exists(string schema = "", string table = "", string where = "", Options options = null, object args = null);
        object Insert(string schema = "", string table = "", string pkField = "", Options options = null, object args = null);

        object Update(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null);
        object Delete(string schema = "", string table = "", string where = "", Options options = null, object args = null);
        string GeneratePocoCode(string commandText, string typeName);
    }
    #endregion Entities
    public static class UniOrmHelperExtensions
    {
        public static DatabaseType GetDatabaseType(this DbProviderFactory dbProviderFactory)
        {
            string providerNameLower = dbProviderFactory.GetType().FullName.ToLowerInvariant();

            DatabaseType dbType = DatabaseType.SQLite;

            if (providerNameLower.Contains("sqlclient"))
                dbType = DatabaseType.SQLServer;
            else if (providerNameLower.Contains("mysql"))
                dbType = DatabaseType.MySQL;
            else if (providerNameLower.Contains("oracle"))
                dbType = DatabaseType.Oracle;
            else if (providerNameLower.Contains("sqlite"))
                dbType = DatabaseType.SQLite;
            else if (providerNameLower.Contains("npgsql"))
                dbType = DatabaseType.PostgreSQL;
            else if (providerNameLower.Contains("sqlserverce"))
                dbType = DatabaseType.SqlServerCE;
            else if (providerNameLower.Contains("oledb"))
                dbType = DatabaseType.OleDB;

            return dbType;
        }
        public static string GetDatabaseParameterPrefix(this DatabaseType databaseType)
        {
            if (databaseType == DatabaseType.MySQL)
                return "?";
            else if (databaseType == DatabaseType.Oracle || databaseType == DatabaseType.PostgreSQL)
                return ":";
            else if (databaseType == DatabaseType.SQLServer || databaseType == DatabaseType.SQLite)
                return "@";
            return null;
        }
        public static Dictionary<string, object> ToParameters(this object obj, string parameterPrefix, string PKFieldToExcept = "", string parameterSuffix = "", string[] includedFields = null)
        {
            Dictionary<string, string> fieldListAsReplace = new Dictionary<string, string>();
            return obj.ToParameters(parameterPrefix, PKFieldToExcept, parameterSuffix, includedFields, out fieldListAsReplace);
        }
        public static Dictionary<string, object> ToParameters(this object obj, string parameterPrefix, string PKFieldToExcept, string parameterSuffix, string[] includedFields, out Dictionary<string, string> fieldListAsReplace)
        {
            fieldListAsReplace = new Dictionary<string, string>();
            var retValue = new Dictionary<string, object>();
            if (obj == null) return retValue;
            Type objType = obj.GetType();
            TypeInfo objTypeInfo = objType.GetTypeInfo();

            var objCol = obj as ICollection;
            var objDict = obj as IDictionary<string, object>;

            if (objCol == null)
            {
                objDict = obj.ToExpando() as IDictionary<string, object>;
                objCol = objDict.ToArray();
            }

            if (objCol != null)
            {
                IEnumerator objEnumerator = objCol.GetEnumerator();
                for (int x = 0; x < objCol.Count; x++)
                {
                    objEnumerator.MoveNext();
                    object objValue = objEnumerator.Current;
                    string parameterName = null;// = x.ToString();

                    if (objValue is ICollection)
                    {
                        retValue = objValue.ToParameters(parameterPrefix, PKFieldToExcept, x.ToString(), includedFields);
                        return retValue;
                    }
                    else if (objValue is KeyValuePair<string, object>)
                    {
                        var objValueAsKeyValuePair = (KeyValuePair<string, object>)objValue;
                        parameterName = objValueAsKeyValuePair.Key;

                        if (objValueAsKeyValuePair.Value is ICollection)
                        {
                            parameterName = parameterPrefix + parameterName;
                            retValue = objValueAsKeyValuePair.Value.ToParameters(parameterName, PKFieldToExcept, "", includedFields);
                            fieldListAsReplace.Add(parameterName, String.Join(",", retValue.Keys));
                            return retValue;
                        }
                        else
                            objValue = objValueAsKeyValuePair.Value;
                    }
                    else if (objValue.IsAnonymous())
                    {
                        objDict = obj.ToExpando() as IDictionary<string, object>;
                        retValue = objDict.ToParameters(parameterPrefix);
                        return retValue;
                    }
                    else
                    {
                        objDict = objValue.ToExpando() as IDictionary<string, object>;
                        if (objDict != null)
                        {
                            var retValue_ = objDict.ToParameters(parameterPrefix, PKFieldToExcept, x.ToString(), includedFields);
                            foreach (var item in retValue_)
                            {
                                retValue.Add(item.Key, item.Value);
                            }

                            if (x < objCol.Count) continue;

                            return retValue;
                        }
                    }

                    if (!String.IsNullOrEmpty(parameterName) && includedFields?.Contains(parameterName) == false) continue;

                    parameterName = parameterName ?? x.ToString();

                    retValue.Add($"{parameterPrefix}{parameterName}{parameterSuffix}", objValue);
                }
            }

            return retValue;
        }
        public static string ToColumnString(this object obj, string PKFieldToExcept = "")
        {
            IDictionary<string, object> objDict = obj.ToExpando();
            return String.Join(",", objDict.Keys.Except(new string[] { PKFieldToExcept }, StringComparer.OrdinalIgnoreCase));
        }
        public static string ToParameterString(this object obj, string parameterPrefix, string PKFieldToExcept = "", string parameterSuffix = "")
        {
            IDictionary<string, object> objDict = obj.ToExpando();
            return String.Join(",", objDict.Keys.Except(new string[] { PKFieldToExcept }, StringComparer.OrdinalIgnoreCase).Select(f => $"{parameterPrefix}{f}{parameterSuffix}"));
        }
        public static string ToColumnAndParameterString(this object obj, string parameterPrefix, string PKFieldToExcept = "", string parameterSuffix = "", string seperator = ",", string[] includedFields = null)
        {
            IDictionary<string, object> objDict = obj.ToExpando();
            var columnParameterList = new List<string>();

            foreach (KeyValuePair<string, object> item in objDict)
            {
                if (item.Key.Equals(PKFieldToExcept, StringComparison.OrdinalIgnoreCase) || includedFields?.Contains(item.Key) == false)
                    continue;

                columnParameterList.Add($"{item.Key}={parameterPrefix}{item.Key}{parameterSuffix}");
            }

            return String.Join(seperator, columnParameterList);
        }
    }
    public static class UniOrmExtensions
    {
        //public static dynamic Execute(this IUniOrm uniOrm, SqlEntity sqlEntity)
        //{
        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    string operationName = sqlEntity.OperationName.ToLowerInvariant();

        //    if (operationName == "query")
        //        return uniOrm.Query(sqlStatement, sqlEntity.Options, sqlEntity);
        //    else if (operationName == "avg" || operationName == "max" || operationName == "min" || operationName == "sum" || operationName == "count" || operationName == "exists" || operationName == "insert" || operationName == "update" || operationName == "delete")
        //        return uniOrm.ExecuteScalar(sqlStatement, sqlEntity.Options, sqlEntity);

        //    return null;
        //}
        //public static IEnumerable<dynamic> Query(this IUniOrm uniOrm, string schema = "", string table = "", string columns = "", string where = "", string orderby = "", string groupby = "", string having = "", int limit = 0, int pageSize = 0, int pageNo = 0, Options options = null, object args = null)
        //{
        //    columns = columns.GetValueIfNotNullorEmpty() ?? "*";
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "QUERY", Schema = schema, Table = table, Columns = columns, Where = where, OrderBy = orderby, GroupBy = groupby, Having = having, Limit = limit, PageSize = pageSize, PageNo = pageNo, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.Query(sqlStatement, options, sqlEntity);
        //}
        //public static IEnumerable<T> Query<T>(this IUniOrm uniOrm, string schema = "", string table = "", string columns = "*", string where = "", string orderby = "", string groupby = "", string having = "", int limit = 0, int pageSize = 0, int pageNo = 0, Options options = null, object args = null) where T : new()
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "QUERY", Schema = schema, Table = table, Columns = columns, Where = where, OrderBy = orderby, GroupBy = groupby, Having = having, Limit = limit, PageSize = pageSize, PageNo = pageNo, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.Query<T>(sqlStatement, options, sqlEntity);
        //}
        //public static object Avg(this IUniOrm uniOrm, string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "AVG", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.ExecuteScalar(sqlStatement, options, sqlEntity);
        //}
        //public static object Max(this IUniOrm uniOrm, string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "MAX", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.ExecuteScalar(sqlStatement, options, sqlEntity);
        //}
        //public static object Min(this IUniOrm uniOrm, string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "MIN", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.ExecuteScalar(sqlStatement, options, sqlEntity);
        //}
        //public static object Sum(this IUniOrm uniOrm, string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "SUM", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.ExecuteScalar(sqlStatement, options, sqlEntity);
        //}
        //public static int Count(this IUniOrm uniOrm, string schema = "", string table = "", string where = "", Options options = null, object args = null)
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "COUNT", Schema = schema, Table = table, Where = where, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.ExecuteScalar<int>(sqlStatement, options, sqlEntity);
        //}
        //public static bool Exists(this IUniOrm uniOrm, string schema = "", string table = "", string where = "", Options options = null, object args = null)
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "EXISTS", Schema = schema, Table = table, Where = where, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.ExecuteScalar<bool>(sqlStatement, options, sqlEntity);
        //}
        //public static object Insert(this IUniOrm uniOrm, string schema = "", string table = "", string pkField = "", Options options = null, object args = null)
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "INSERT", Schema = schema, Table = table, PKField = pkField, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.ExecuteScalar(sqlStatement, options, sqlEntity);
        //}
        //public static object Update(this IUniOrm uniOrm, string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "UPDATE", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.ExecuteScalar(sqlStatement, options, sqlEntity);
        //}
        //public static object Delete(this IUniOrm uniOrm, string schema = "", string table = "", string where = "", Options options = null, object args = null)
        //{
        //    SqlEntity sqlEntity = new SqlEntity { OperationName = "DELETE", Schema = schema, Table = table, Where = where, DatabaseType = uniOrm.DatabaseType, Args = args };

        //    string sqlStatement = SqlEntity.BuildSql(sqlEntity);

        //    return uniOrm.ExecuteScalar(sqlStatement, options, sqlEntity);
        //}
        //public static string GeneratePocoCode(this IUniOrm uniOrm, string commandText, string typeName)
        //{
        //    string result;
        //    using (DbConnection con = uniOrm.NewConnection())
        //    using (DbCommand com = uniOrm.NewCommand(con: con, commandText: commandText))
        //    using (DbDataReader reader = com.ExecuteReader())
        //        result = UniCornTypeFactory.GeneratePocoCode(reader, typeName, true, true);
        //    return result;
        //}
    }
    public partial class UniOrm : IUniOrm
    {
        #region Field Members
        DbProviderFactory _dbProviderFactory;
        Options _defaultOptions;
        static MethodInfo queryGenericMethodInfo = typeof(UniCorn.Orm.UniOrm).GetMethods().FirstOrDefault(f => f.Name == "Query" && f.IsGenericMethod);
        #endregion Field Members
        #region Properties
        public string ConnectionString { get; private set; }
        public string ProviderName { get; private set; }
        public DatabaseType DatabaseType { get; private set; }
        public string ParameterPrefix { get; private set; }
        public Options DefaultOptions
        {
            get { return _defaultOptions; }
            set
            {
                if (value == null)
                    throw new ArgumentNullException("DefaultOptions");
                _defaultOptions = value;
            }
        }
        #endregion Properties
        #region Constructors
        public UniOrm(string connectionStringName = "ConnectionStrings:DefaultConnection:ConnectionString", DbProviderFactory dbProviderFactory = null, Options defaultOptions = null)
        {
            if (String.IsNullOrEmpty(connectionStringName))
                throw new ArgumentNullException("connectionStringName");

            _dbProviderFactory = dbProviderFactory;

            IConfiguration config = new ConfigurationBuilder()
                                           .SetBasePath(Directory.GetCurrentDirectory())
                                           .AddJsonFile("appsettings.json", true, true)
                                           .Build();

            ConnectionString = config[connectionStringName].GetValueIfNotNullorEmpty() ?? config[connectionStringName + ":ConnectionString"];

            if (String.IsNullOrEmpty(ConnectionString))
                throw new NullReferenceException("ConnectionString");

            ProviderName = config[connectionStringName + ":ProviderName"];

            if (dbProviderFactory == null && !String.IsNullOrEmpty(ProviderName))
            {
                Assembly assembly = Assembly.Load(new AssemblyName(ProviderName));

                Type sqliteFactory = assembly.GetTypes().FirstOrDefault(f => f.Name.ToLowerInvariant().Contains("factory"));

                FieldInfo instanceOfDbProviderFactoryFi = sqliteFactory.GetField("Instance");

                _dbProviderFactory = instanceOfDbProviderFactoryFi.GetValue(null) as DbProviderFactory;
            }

            if (_dbProviderFactory == null)
                throw new ArgumentNullException("dbProviderFactory");

            bool isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            if (!isWindows)
                ConnectionString = ConnectionString.Replace('\\', Path.DirectorySeparatorChar);

            DatabaseType = _dbProviderFactory.GetDatabaseType();

            DefaultOptions = defaultOptions == null ? new Options() : defaultOptions;

            ParameterPrefix = DatabaseType.GetDatabaseParameterPrefix();
        }
        #endregion Constructors
        #region Database Operations
        private dynamic GetOutputParameters(DbCommand com)
        {
            dynamic retValue = new ExpandoObject();
            var retValueDict = retValue as IDictionary<string, object>;
            foreach (DbParameter parameter in com.Parameters)
                if (parameter.Direction == ParameterDirection.Output || parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.ReturnValue)
                    retValueDict[parameter.ParameterName] = parameter.Value;
            return retValue;
        }
        public virtual DbConnection NewConnection()
        {
            DbConnection con = _dbProviderFactory.CreateConnection();
            con.ConnectionString = ConnectionString;
            con.Open();
            return con;
        }
        public virtual DbCommand NewCommand(DbConnection con, string commandText, Options options = null, object args = null)
        {
            if (String.IsNullOrEmpty(commandText))
                throw new ArgumentNullException("commandText");

            DbCommand com = _dbProviderFactory.CreateCommand();

            com.Connection = con;
            com.CommandTimeout = 180;
            com.CommandText = commandText;

            SqlEntity sqlEntity = args as SqlEntity;
            string[] includedFields = null;
            string pkField = null;

            if (sqlEntity != null)
            {
                args = sqlEntity.Args;
                includedFields = sqlEntity.WhereParameterArray?.Union(sqlEntity.ColumnArray).ToArray();
                pkField = sqlEntity.PKField;
            }

            Dictionary<string, string> fieldListAsReplace;
            Dictionary<string, object> parameterDict = args.ToParameters(ParameterPrefix, pkField, "", includedFields, out fieldListAsReplace);

            if (fieldListAsReplace != null)
                foreach (var item in fieldListAsReplace)
                    com.CommandText = com.CommandText.Replace(item.Key, $"({item.Value})");

            foreach (var parameter in parameterDict)
                com.Parameters.Add(NewParameter(com, parameter.Key, parameter.Value, options: options));

            Action<DbCommand> onCommandPreExecuting = DefaultOptions.EventListener.OnCommandPreExecuting;

            if (options != null)
                onCommandPreExecuting += options.EventListener.OnCommandPreExecuting;

            onCommandPreExecuting?.Invoke(com);

            return com;
        }
        public virtual DbParameter NewParameter(DbCommand com, string parameterName, object value, DbType dbType = System.Data.DbType.Object, ParameterDirection parameterDirection = ParameterDirection.Input, Options options = null)
        {
            DbParameter parameter = com.CreateParameter();
            parameter.ParameterName = parameterName;
            parameter.Value = (value ?? DBNull.Value);
            parameter.Direction = parameterDirection;
            if (parameterDirection == ParameterDirection.Output)//It will be also tested for input type
                parameter.DbType = dbType;
            return parameter;
        }
        public virtual IEnumerable<dynamic> ExecuteReader(string commandText = "", Options options = null, object args = null)
        {
            //List<dynamic> result = new List<dynamic>();
            Stopwatch sw = Stopwatch.StartNew();
            int rowCount = 0;
            dynamic outputParameters = null;
            using (DbConnection con = NewConnection())
            using (DbCommand com = NewCommand(con: con, commandText: commandText, options: options, args: args))
            {
                using (DbDataReader reader = com.ExecuteReader())
                {
                    Func<DbDataReader, ExpandoObject> expandoObjectMap = reader.ToExpandoObjectMapMethod();

                    while (reader.Read())
                    {
                        rowCount++;
                        yield return expandoObjectMap(reader);
                    }
                    //result.Add(expandoObjectMap(reader));
                }

                commandText = com.CommandText;
                outputParameters = GetOutputParameters(com);
            }

            sw.Stop();
            Action<Callback> onCallback = DefaultOptions.EventListener.OnCallback;

            if (options != null)
                onCallback += options.EventListener.OnCallback;

            if (onCallback != null)
                onCallback(new Callback { SqlQuery = commandText, OutputParameters = outputParameters, ElapsedTime = sw.Elapsed, RowCount = rowCount });

            //return result;
        }
        public virtual IEnumerable<object> ExecuteReader(Type type, string commandText = "", Options options = null, object args = null)
        {
            MethodInfo queryMethod = queryGenericMethodInfo.MakeGenericMethod(type);

            return queryMethod.Invoke(this, new object[] { commandText, options, args }) as IEnumerable<object>;
        }
        public virtual IEnumerable<T> ExecuteReader<T>(string commandText = "", Options options = null, object args = null) where T : new()
        {
            //List<T> result = new List<T>();
            Stopwatch sw = Stopwatch.StartNew();
            int rowCount = 0;
            dynamic outputParameters = null;
            using (DbConnection con = NewConnection())
            using (DbCommand com = NewCommand(con: con, commandText: commandText, options: options, args: args))
            {
                using (DbDataReader reader = com.ExecuteReader())
                {
                    Func<DbDataReader, T> entityMap = reader.ToEntityMapMethod<T>();
                    while (reader.Read())
                    {
                        rowCount++;
                        yield return entityMap(reader);
                    }
                    //result.Add(entityMap(reader));
                }

                commandText = com.CommandText;
                outputParameters = GetOutputParameters(com);
            }

            sw.Stop();
            Action<Callback> onCallback = DefaultOptions.EventListener.OnCallback;

            if (options != null)
                onCallback += options.EventListener.OnCallback;

            if (onCallback != null)
                onCallback(new Callback { SqlQuery = commandText, OutputParameters = outputParameters, ElapsedTime = sw.Elapsed, RowCount = rowCount });

            //return result;
        }
        public virtual object ExecuteScalar(string commandText = "", Options options = null, object args = null)
        {
            object result = default(object);
            dynamic outputParameters = null;
            Stopwatch sw = Stopwatch.StartNew();
            DbConnection con = options?.Transaction?.Connection ?? NewConnection();
            using (con)
            using (DbCommand com = NewCommand(con: con, commandText: commandText, options: options, args: args))
            {
                result = com.ExecuteScalar();

                commandText = com.CommandText;
                outputParameters = GetOutputParameters(com);
            }

            sw.Stop();
            Action<Callback> onCallback = DefaultOptions.EventListener.OnCallback;

            if (options != null)
                onCallback += options.EventListener.OnCallback;

            if (onCallback != null)
                onCallback(new Callback { SqlQuery = commandText, OutputParameters = outputParameters, ElapsedTime = sw.Elapsed });

            return result;
        }
        public virtual T ExecuteScalar<T>(string commandText = "", Options options = null, object args = null) where T : new()
        {
            object result = ExecuteScalar(commandText: commandText, options: options, args: args);
            options = options ?? DefaultOptions;
            return result.To<T>(fieldNameLower: options.FieldNameLower);
        }
        public virtual int ExecuteNonQuery(string commandText = "", Options options = null, object args = null)
        {
            int result = default(int);
            dynamic outputParameters = null;
            Stopwatch sw = Stopwatch.StartNew();
            DbConnection con = options?.Transaction?.Connection ?? NewConnection();
            using (con)
            using (DbCommand com = NewCommand(con: con, commandText: commandText, options: options, args: args))
            {
                result = com.ExecuteNonQuery();

                commandText = com.CommandText;
                outputParameters = GetOutputParameters(com);
            }

            sw.Stop();
            Action<Callback> onCallback = DefaultOptions.EventListener.OnCallback;

            if (options != null)
                onCallback += options.EventListener.OnCallback;

            if (onCallback != null)
                onCallback(new Callback { SqlQuery = commandText, OutputParameters = outputParameters, ElapsedTime = sw.Elapsed });

            return result;
        }
        #endregion Database Operations
    }
    public partial class UniOrm
    {
        public dynamic Execute(SqlEntity sqlEntity)
        {
            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            string operationName = sqlEntity.OperationName.ToLowerInvariant();
            
            if (operationName == "query")
                return ExecuteReader(sqlStatement, sqlEntity.Options, sqlEntity);
            else if (operationName == "avg" || operationName == "max" || operationName == "min" || operationName == "sum" || operationName == "count" || operationName == "exists" || operationName == "insert" || operationName == "update" || operationName == "delete")
                return ExecuteScalar(sqlStatement, sqlEntity.Options, sqlEntity);

            return null;
        }
        public IEnumerable<dynamic> Query(string sql = "", string schema = "", string table = "", string columns = "", string where = "", string orderby = "", string groupby = "", string having = "", int limit = 0, int pageSize = 0, int pageNo = 0, Options options = null, object args = null)
        {
            columns = columns.GetValueIfNotNullorEmpty() ?? "*";
            SqlEntity sqlEntity = new SqlEntity { OperationName = "QUERY", Sql = sql, Schema = schema, Table = table, Columns = columns, Where = where, OrderBy = orderby, GroupBy = groupby, Having = having, Limit = limit, PageSize = pageSize, PageNo = pageNo, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteReader(sqlStatement, options, sqlEntity);
        }
        public IEnumerable<T> Query<T>(string sql = "", string schema = "", string table = "", string columns = "*", string where = "", string orderby = "", string groupby = "", string having = "", int limit = 0, int pageSize = 0, int pageNo = 0, Options options = null, object args = null) where T : new()
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "QUERY", Sql = sql, Schema = schema, Table = table, Columns = columns, Where = where, OrderBy = orderby, GroupBy = groupby, Having = having, Limit = limit, PageSize = pageSize, PageNo = pageNo, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteReader<T>(sqlStatement, options, sqlEntity);
        }
        public object Avg(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "AVG", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteScalar(sqlStatement, options, sqlEntity);
        }
        public object Max(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "MAX", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteScalar(sqlStatement, options, sqlEntity);
        }
        public object Min(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "MIN", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteScalar(sqlStatement, options, sqlEntity);
        }
        public object Sum(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "SUM", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteScalar(sqlStatement, options, sqlEntity);
        }
        public int Count(string schema = "", string table = "", string where = "", Options options = null, object args = null)
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "COUNT", Schema = schema, Table = table, Where = where, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteScalar<int>(sqlStatement, options, sqlEntity);
        }
        public bool Exists(string schema = "", string table = "", string where = "", Options options = null, object args = null)
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "EXISTS", Schema = schema, Table = table, Where = where, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteScalar<bool>(sqlStatement, options, sqlEntity);
        }
        public object Insert(string schema = "", string table = "", string pkField = "", Options options = null, object args = null)
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "INSERT", Schema = schema, Table = table, PKField = pkField, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteScalar(sqlStatement, options, sqlEntity);
        }
        public object Update(string schema = "", string table = "", string columns = "", string where = "", Options options = null, object args = null)
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "UPDATE", Schema = schema, Table = table, Columns = columns, Where = where, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteScalar(sqlStatement, options, sqlEntity);
        }
        public object Delete(string schema = "", string table = "", string where = "", Options options = null, object args = null)
        {
            SqlEntity sqlEntity = new SqlEntity { OperationName = "DELETE", Schema = schema, Table = table, Where = where, DatabaseType = this.DatabaseType, Args = args };

            string sqlStatement = SqlEntity.BuildSql(sqlEntity);

            return ExecuteScalar(sqlStatement, options, sqlEntity);
        }
        public string GeneratePocoCode(string commandText, string typeName)
        {
            string result;
            using (DbConnection con = NewConnection())
            using (DbCommand com = NewCommand(con: con, commandText: commandText))
            using (DbDataReader reader = com.ExecuteReader())
                result = UniCornTypeFactory.GeneratePocoCode(reader, typeName, true, true);
            return result;
        }
    }
}